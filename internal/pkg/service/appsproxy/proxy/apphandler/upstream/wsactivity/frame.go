// Package wsactivity provides a passive WebSocket frame observer used by the apps-proxy
// to track real per-frame activity instead of the mere presence of an open connection.
//
// The package contains two pieces:
//
//   - a stateful RFC 6455 frame parser (frame.go) that consumes a raw byte stream and
//     invokes a callback exactly once per non-control frame ("data frame"), and
//   - a passthrough net-conn wrapper (conn.go) that drives one parser per direction.
//
// The parser does not inspect or buffer payload bytes; it only buffers the frame
// header (at most 14 bytes per RFC 6455) and decrements a payload counter as bytes
// flow through. This makes it safe against attacker-controlled payload lengths
// (which can reach 2^63-1 per RFC 6455).
package wsactivity

import "encoding/binary"

// FrameCallback is invoked once per non-control frame (opcode bit 0x8 unset)
// at the moment its header has been fully parsed. Implementations must be
// cheap and non-blocking — the callback runs on the goroutine that reads or
// writes bytes through the wrapped connection.
type FrameCallback func()

const (
	// maxHeaderBytes is the upper bound on a single WebSocket frame header:
	//   2 base bytes + up to 8 extended-length bytes + up to 4 mask-key bytes.
	maxHeaderBytes = 14

	// maskBit is the high bit of byte 1.
	maskBit = 0x80
	// lenMask is the low 7 bits of byte 1 (the 7-bit payload-length field).
	lenMask = 0x7F
	// controlBit identifies control frames per RFC 6455 §5.2 (opcode & 0x8).
	controlBit = 0x8

	// payloadLen16 and payloadLen64 are the sentinel values of the 7-bit length
	// field that select 16-bit and 64-bit extended payload-length encodings.
	payloadLen16 = 126
	payloadLen64 = 127
)

// frameParser is a stateful, allocation-free RFC 6455 frame header parser.
//
// It is fed an arbitrary byte stream via Feed and invokes onDataFrame exactly
// once per non-control frame (opcodes 0x0..0x7), at the moment the frame
// header is fully parsed. Payload bytes are not inspected — they are skipped
// via a counter.
//
// Implicit state is encoded in three fields:
//   - payloadLeft > 0 → currently draining a payload (header already fired);
//   - headerNeed == 0 → waiting for the first 2 header bytes to know total
//     header length;
//   - headerLen < headerNeed → still accumulating header bytes.
//
// The parser holds no resync logic. A malformed stream desynchronizes the
// parser indefinitely; the connection endpoints (browser, upstream) will
// terminate such streams on their own and the throttling in notify.Manager
// caps the cost of any spurious callback bursts.
type frameParser struct {
	onDataFrame FrameCallback

	// header buffers the current frame's header bytes. Indexed via headerLen,
	// decoded all at once when headerLen reaches headerNeed.
	header     [maxHeaderBytes]byte
	headerLen  int
	headerNeed int // 0 until the 7-bit length field has been read

	payloadLeft uint64 // bytes of the current payload still to drain
}

// newFrameParser returns a parser that calls onDataFrame once per non-control frame.
func newFrameParser(onDataFrame FrameCallback) *frameParser {
	return &frameParser{onDataFrame: onDataFrame}
}

// Feed advances the parser through the bytes in p. It never reads or modifies
// p beyond walking its bytes and never allocates based on payload length.
func (fp *frameParser) Feed(p []byte) {
	for len(p) > 0 {
		// Phase 1: drain payload of the current frame, if any. Once payloadLeft
		// hits zero the parser is back at the start of a new header.
		if fp.payloadLeft > 0 {
			p = fp.drainPayload(p)
			continue
		}

		// Phase 2: accumulate at least the first 2 bytes of the next header so
		// we can compute the header's total length.
		if fp.headerNeed == 0 {
			p = fp.fillHeader(p, 2)
			if fp.headerLen < 2 {
				return // need more bytes; come back on the next Feed call
			}
			fp.headerNeed = headerLengthFromByte1(fp.header[1])
		}

		// Phase 3: finish accumulating the rest of the header.
		p = fp.fillHeader(p, fp.headerNeed)
		if fp.headerLen < fp.headerNeed {
			return // need more bytes; come back on the next Feed call
		}

		fp.completeHeader()
	}
}

// drainPayload consumes up to payloadLeft bytes from the front of p and returns
// the rest. Bytes are not inspected.
func (fp *frameParser) drainPayload(p []byte) []byte {
	n := uint64(len(p))
	if n > fp.payloadLeft {
		n = fp.payloadLeft
	}
	fp.payloadLeft -= n
	return p[n:]
}

// fillHeader copies bytes from p into the header buffer until either p is
// exhausted or headerLen reaches target. Returns the remaining slice of p.
func (fp *frameParser) fillHeader(p []byte, target int) []byte {
	if fp.headerLen >= target {
		return p
	}
	n := copy(fp.header[fp.headerLen:target], p)
	fp.headerLen += n
	return p[n:]
}

// headerLengthFromByte1 returns the total header length implied by byte 1 of
// the frame (the MASK bit + 7-bit length field). Result is in [2, 14].
func headerLengthFromByte1(b byte) int {
	n := 2
	switch b & lenMask {
	case payloadLen16:
		n += 2
	case payloadLen64:
		n += 8
	}
	if b&maskBit != 0 {
		n += 4
	}
	return n
}

// completeHeader decodes the buffered header, fires the data-frame callback
// (if applicable), primes payloadLeft for the next drain phase, and resets
// header accumulation for the next frame.
func (fp *frameParser) completeHeader() {
	opcode := fp.header[0] & 0x0F
	b1 := fp.header[1]

	var payloadLen uint64
	switch b1 & lenMask {
	case payloadLen16:
		payloadLen = uint64(binary.BigEndian.Uint16(fp.header[2:4]))
	case payloadLen64:
		payloadLen = binary.BigEndian.Uint64(fp.header[2:10])
	default:
		payloadLen = uint64(b1 & lenMask)
	}
	// The mask key, if present, sits in the last 4 bytes of the header and is
	// of no interest to the observer — we only need its length, which is
	// already accounted for in headerLengthFromByte1.

	if opcode&controlBit == 0 && fp.onDataFrame != nil {
		fp.onDataFrame()
	}

	fp.payloadLeft = payloadLen
	fp.headerLen = 0
	fp.headerNeed = 0
}
