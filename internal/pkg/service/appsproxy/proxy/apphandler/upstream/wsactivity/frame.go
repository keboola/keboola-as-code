// Package wsactivity provides a passive WebSocket frame observer used by the apps-proxy
// to track real per-frame activity instead of the mere presence of an open connection.
//
// The package contains two pieces:
//
//   - a stateful RFC 6455 frame parser (frame.go) that consumes a raw byte stream and
//     invokes a callback exactly once per non-control frame ("data frame"), and
//   - a passthrough net-conn wrapper (conn.go) that drives one parser per direction.
//
// The parser does not inspect or buffer payload bytes; it only walks frame headers and
// decrements a payload counter as bytes flow through. This makes it safe against
// attacker-controlled payload lengths (which can reach 2^63-1 per RFC 6455).
package wsactivity

// FrameCallback is invoked once per non-control frame (opcode bit 0x8 unset)
// at the moment its header has been fully parsed. Implementations must be
// cheap and non-blocking — the callback runs on the goroutine that reads or
// writes bytes through the wrapped connection.
type FrameCallback func()

// parserState enumerates the positions of the RFC 6455 frame state machine.
type parserState uint8

const (
	stateHeader0 parserState = iota // expect byte 0: FIN/RSV/OPCODE
	stateHeader1                    // expect byte 1: MASK/PAYLOAD_LEN_7
	stateExtLen                     // accumulate 2 or 8 extended length bytes
	stateMaskKey                    // accumulate 4 masking-key bytes (only if MASK=1)
	statePayload                    // drain payload bytes
)

const (
	// maskBit is the high bit of byte 1.
	maskBit = 0x80
	// lenMask is the low 7 bits of byte 1.
	lenMask = 0x7F
	// controlBit identifies control frames per RFC 6455 §5.2 (opcode & 0x8).
	controlBit = 0x8
)

// frameParser is a stateful, allocation-free RFC 6455 frame header parser.
//
// It is fed an arbitrary byte stream via Feed and invokes onDataFrame exactly
// once per non-control frame (opcodes 0x0..0x7), at the moment the frame
// header is fully parsed. Payload bytes are not buffered or inspected — they
// are skipped via a counter.
//
// The parser holds no resync logic. A malformed stream desynchronizes the
// parser indefinitely; the connection endpoints (browser, upstream) will
// terminate such streams on their own and the throttling in notify.Manager
// caps the cost of any spurious callback bursts.
type frameParser struct {
	onDataFrame FrameCallback

	state parserState

	// Current frame metadata, populated as the header is parsed.
	opcode      byte
	masked      bool
	extLenBytes uint8  // 0, 2, or 8
	extLenRead  uint8  // bytes of extLen consumed so far
	extLenAccum uint64 // accumulated extended-length value
	maskKeyRead uint8  // 0..4
	payloadLeft uint64 // bytes of payload still to drain
}

// newFrameParser returns a parser that calls onDataFrame once per non-control frame.
func newFrameParser(onDataFrame FrameCallback) *frameParser {
	return &frameParser{onDataFrame: onDataFrame}
}

// Feed advances the parser through the bytes in p. It never reads or modifies
// p beyond walking its bytes and never allocates based on payload length.
func (fp *frameParser) Feed(p []byte) {
	for i := 0; i < len(p); {
		switch fp.state {
		case stateHeader0:
			fp.opcode = p[i] & 0x0F
			fp.state = stateHeader1
			i++

		case stateHeader1:
			b := p[i]
			fp.masked = b&maskBit != 0
			length7 := b & lenMask
			switch {
			case length7 < 126:
				fp.extLenBytes = 0
				fp.payloadLeft = uint64(length7)
			case length7 == 126:
				fp.extLenBytes = 2
			default: // 127
				fp.extLenBytes = 8
			}
			fp.extLenRead = 0
			fp.extLenAccum = 0
			fp.maskKeyRead = 0
			if fp.extLenBytes == 0 {
				fp.afterLengthKnown()
			} else {
				fp.state = stateExtLen
			}
			i++

		case stateExtLen:
			// Consume as many extLen bytes as available in p[i:].
			need := int(fp.extLenBytes - fp.extLenRead)
			avail := len(p) - i
			n := need
			if avail < n {
				n = avail
			}
			for k := 0; k < n; k++ {
				fp.extLenAccum = (fp.extLenAccum << 8) | uint64(p[i+k])
			}
			fp.extLenRead += uint8(n)
			i += n
			if fp.extLenRead == fp.extLenBytes {
				fp.payloadLeft = fp.extLenAccum
				fp.afterLengthKnown()
			}

		case stateMaskKey:
			need := int(4 - fp.maskKeyRead)
			avail := len(p) - i
			n := need
			if avail < n {
				n = avail
			}
			fp.maskKeyRead += uint8(n)
			i += n
			if fp.maskKeyRead == 4 {
				fp.afterHeaderComplete()
			}

		case statePayload:
			avail := uint64(len(p) - i)
			drop := avail
			if fp.payloadLeft < drop {
				drop = fp.payloadLeft
			}
			fp.payloadLeft -= drop
			i += int(drop)
			if fp.payloadLeft == 0 {
				fp.state = stateHeader0
			}
		}
	}
}

// afterLengthKnown is called once payload length is known (either after byte 1
// for 7-bit length, or after the extended-length bytes for 16/64-bit length).
// It advances to either stateMaskKey or directly to header-complete handling.
func (fp *frameParser) afterLengthKnown() {
	if fp.masked {
		fp.state = stateMaskKey
		return
	}
	fp.afterHeaderComplete()
}

// afterHeaderComplete is called when the entire frame header has been parsed.
// It fires the data-frame callback (if applicable) and transitions to payload
// drain or directly back to stateHeader0 for zero-length payloads.
func (fp *frameParser) afterHeaderComplete() {
	// Non-control frames have the high bit of the 4-bit opcode unset.
	if fp.opcode&controlBit == 0 && fp.onDataFrame != nil {
		fp.onDataFrame()
	}
	if fp.payloadLeft == 0 {
		fp.state = stateHeader0
		return
	}
	fp.state = statePayload
}
