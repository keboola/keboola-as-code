package wsactivity

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

// buildFrame constructs a raw RFC 6455 frame with the given opcode, FIN bit,
// payload length, and (optionally) a 4-byte masking key. Payload bytes are
// filled with deterministic data; the parser never inspects them, so the
// content is irrelevant.
func buildFrame(t *testing.T, opcode byte, fin bool, payloadLen uint64, masked bool) []byte {
	t.Helper()

	var buf []byte

	// Byte 0: FIN | RSV1..3 | OPCODE
	b0 := opcode & 0x0F
	if fin {
		b0 |= 0x80
	}
	buf = append(buf, b0)

	// Byte 1: MASK | PAYLOAD_LEN_7 + extended length
	var b1 byte
	if masked {
		b1 |= maskBit
	}
	switch {
	case payloadLen < 126:
		b1 |= byte(payloadLen)
		buf = append(buf, b1)
	case payloadLen <= 0xFFFF:
		b1 |= 126
		buf = append(buf, b1)
		extLen := make([]byte, 2)
		binary.BigEndian.PutUint16(extLen, uint16(payloadLen))
		buf = append(buf, extLen...)
	default:
		b1 |= 127
		buf = append(buf, b1)
		extLen := make([]byte, 8)
		binary.BigEndian.PutUint64(extLen, payloadLen)
		buf = append(buf, extLen...)
	}

	// 4-byte masking key (content arbitrary; parser only counts bytes).
	if masked {
		buf = append(buf, 0xDE, 0xAD, 0xBE, 0xEF)
	}

	// Payload bytes (parser only walks past them via the counter).
	if payloadLen > 0 {
		payload := make([]byte, payloadLen)
		for i := range payload {
			payload[i] = byte(i)
		}
		buf = append(buf, payload...)
	}

	return buf
}

// countCallbacks feeds the given byte slices (in order) into a fresh parser
// and returns the total number of data-frame callbacks observed.
func countCallbacks(chunks ...[]byte) int {
	count := 0
	p := newFrameParser(func() { count++ })
	for _, c := range chunks {
		p.Feed(c)
	}
	return count
}

func TestFrameParser_SingleTextFrame_FixedPayload(t *testing.T) {
	t.Parallel()
	frame := buildFrame(t, 0x1, true, 5, false)
	assert.Equal(t, 1, countCallbacks(frame))
}

func TestFrameParser_ExtLen16(t *testing.T) {
	t.Parallel()
	for _, payloadLen := range []uint64{126, 200, 65535} {
		frame := buildFrame(t, 0x2, true, payloadLen, false)
		assert.Equal(t, 1, countCallbacks(frame), "payloadLen=%d", payloadLen)
	}
}

func TestFrameParser_ExtLen64(t *testing.T) {
	t.Parallel()
	// 65536 = first value requiring 64-bit ext length.
	frame := buildFrame(t, 0x2, true, 65536, false)
	assert.Equal(t, 1, countCallbacks(frame))
}

func TestFrameParser_ExtLen64_NoAllocation_HugeLen(t *testing.T) {
	t.Parallel()
	// Header-only feed with a 64-bit length larger than any plausible
	// allocation. The parser must accept and transition to payload-drain
	// without allocating a buffer for the payload.
	frame := buildFrame(t, 0x2, true, 0, false) // baseline header
	// Patch the 7-bit length field to 127 and append a huge 64-bit length.
	huge := uint64(1) << 50 // 1 PiB — clearly non-allocatable
	extLen := make([]byte, 8)
	binary.BigEndian.PutUint64(extLen, huge)
	patched := make([]byte, 0, 2+len(extLen))
	patched = append(patched, frame[0], 127)
	patched = append(patched, extLen...)

	count := 0
	p := newFrameParser(func() { count++ })
	p.Feed(patched)
	assert.Equal(t, 1, count, "callback must fire once on header completion")
	// Header is fully consumed and parser is now draining the payload — never
	// allocating a buffer for it, just decrementing payloadLeft as bytes arrive.
	assert.Equal(t, huge, p.payloadLeft, "must record full payload length")
	assert.Equal(t, 0, p.headerLen, "header buffer must be reset after header completion")
	assert.Equal(t, 0, p.headerNeed, "headerNeed must be reset after header completion")
}

func TestFrameParser_MaskedFrame(t *testing.T) {
	t.Parallel()
	// Client-side frame with MASK=1 and a 4-byte key — parser must skip the
	// key and still emit the callback exactly once.
	frame := buildFrame(t, 0x1, true, 10, true)
	assert.Equal(t, 1, countCallbacks(frame))
}

func TestFrameParser_ControlFramesIgnored(t *testing.T) {
	t.Parallel()
	for _, opcode := range []byte{0x8 /* close */, 0x9 /* ping */, 0xA /* pong */} {
		// Ping/pong typically use small or zero-length payloads.
		frame := buildFrame(t, opcode, true, 0, false)
		assert.Equal(t, 0, countCallbacks(frame), "opcode=0x%X", opcode)
	}
}

func TestFrameParser_ControlFrame_WithPayload(t *testing.T) {
	t.Parallel()
	// Ping with a payload — still control, still zero callbacks.
	// Tests that the payload-drain path doesn't accidentally fire.
	frame := buildFrame(t, 0x9, true, 50, true)
	assert.Equal(t, 0, countCallbacks(frame))
}

func TestFrameParser_FragmentedMessage_OneCallbackPerFrame(t *testing.T) {
	t.Parallel()
	// text(FIN=0) + continuation(FIN=0) + continuation(FIN=1) = 3 frames.
	// Spec: "Callback fires once per frame at frame-start, not per byte."
	first := buildFrame(t, 0x1, false, 4, false)
	mid := buildFrame(t, 0x0, false, 4, false)
	last := buildFrame(t, 0x0, true, 4, false)
	assert.Equal(t, 3, countCallbacks(first, mid, last))
}

func TestFrameParser_MultipleFramesInOneFeed(t *testing.T) {
	t.Parallel()
	// Three concatenated text frames in a single Feed → 3 callbacks.
	f1 := buildFrame(t, 0x1, true, 3, false)
	f2 := buildFrame(t, 0x2, true, 10, false)
	f3 := buildFrame(t, 0x1, true, 0, false)
	concat := append(append(f1, f2...), f3...)
	assert.Equal(t, 3, countCallbacks(concat))
}

func TestFrameParser_SplitAcrossFeeds(t *testing.T) {
	t.Parallel()
	frame := buildFrame(t, 0x1, true, 200, true) // 16-bit length + mask

	splitPoints := []struct {
		name string
		at   int
	}{
		{"after byte 0", 1},
		{"after byte 1", 2},
		{"middle of extLen", 3},
		{"after extLen", 4},
		{"middle of mask key", 6},
		{"after header", 8},
		{"middle of payload", 100},
	}

	for _, sp := range splitPoints {
		t.Run(sp.name, func(t *testing.T) {
			t.Parallel()
			if sp.at >= len(frame) {
				t.Skipf("split point %d out of range", sp.at)
			}
			assert.Equal(t, 1, countCallbacks(frame[:sp.at], frame[sp.at:]),
				"single frame split at %d should still yield one callback", sp.at)
		})
	}
}

func TestFrameParser_OneByteAtATime(t *testing.T) {
	t.Parallel()
	// Worst-case TCP framing: each byte arrives in its own Feed call.
	frame := buildFrame(t, 0x1, true, 65535, true) // 16-bit length + mask + 65535B payload
	count := 0
	p := newFrameParser(func() { count++ })
	for _, b := range frame {
		p.Feed([]byte{b})
	}
	assert.Equal(t, 1, count)
}

func TestFrameParser_ReservedDataOpcodesCountAsData(t *testing.T) {
	t.Parallel()
	// RFC 6455 §5.2: opcodes 0x3..0x7 are reserved for further non-control
	// frames. The spec asks us to discriminate by (opcode & 0x8), so all of
	// 0x0..0x7 count as data.
	for _, opcode := range []byte{0x3, 0x4, 0x5, 0x6, 0x7} {
		frame := buildFrame(t, opcode, true, 1, false)
		assert.Equal(t, 1, countCallbacks(frame), "opcode=0x%X", opcode)
	}
}

func TestFrameParser_ReservedControlOpcodesIgnored(t *testing.T) {
	t.Parallel()
	// 0xB..0xF are reserved for further control frames — must not callback.
	for _, opcode := range []byte{0xB, 0xC, 0xD, 0xE, 0xF} {
		frame := buildFrame(t, opcode, true, 0, false)
		assert.Equal(t, 0, countCallbacks(frame), "opcode=0x%X", opcode)
	}
}

func TestFrameParser_MixedDataAndControl(t *testing.T) {
	t.Parallel()
	// Realistic Streamlit idle pattern: ping/pong heartbeats interleaved
	// with a single data frame.
	ping := buildFrame(t, 0x9, true, 0, false)
	pong := buildFrame(t, 0xA, true, 0, false)
	data := buildFrame(t, 0x1, true, 12, false)

	stream := make([]byte, 0, 3*len(ping)+len(pong)*2+len(data))
	stream = append(stream, ping...)
	stream = append(stream, pong...)
	stream = append(stream, ping...)
	stream = append(stream, data...)
	stream = append(stream, pong...)

	assert.Equal(t, 1, countCallbacks(stream))
}

func TestFrameParser_EmptyPayload(t *testing.T) {
	t.Parallel()
	// Zero-length text frame: callback fires once, parser returns to header0.
	f1 := buildFrame(t, 0x1, true, 0, false)
	f2 := buildFrame(t, 0x1, true, 3, false)
	assert.Equal(t, 2, countCallbacks(f1, f2),
		"empty-payload frame must not stall the parser")
}

func TestFrameParser_NilCallback_DoesNotPanic(t *testing.T) {
	t.Parallel()
	p := newFrameParser(nil)
	frame := buildFrame(t, 0x1, true, 5, false)
	assert.NotPanics(t, func() { p.Feed(frame) })
}

// FuzzFrameParser feeds arbitrary byte streams to the parser and ensures it
// never panics. The parser is a passive observer — any sequence of bytes must
// be safely walkable.
func FuzzFrameParser(f *testing.F) {
	f.Add([]byte{0x81, 0x05, 'h', 'e', 'l', 'l', 'o'})
	f.Add([]byte{0x89, 0x00})                                                                               // ping
	f.Add([]byte{0x82, 0x7E, 0x00, 0x10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})                   // 16-bit len
	f.Add([]byte{0x82, 0xFF, 0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xAA}) // masked + 64-bit len header
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF})                                                                   // garbage
	f.Add([]byte{})                                                                                         // empty

	f.Fuzz(func(t *testing.T, data []byte) {
		p := newFrameParser(func() {})
		// Feed in random chunk sizes to exercise split-state transitions.
		for i := 0; i < len(data); {
			step := 1 + (int(data[i]) % 7)
			if i+step > len(data) {
				step = len(data) - i
			}
			p.Feed(data[i : i+step])
			i += step
		}
	})
}
