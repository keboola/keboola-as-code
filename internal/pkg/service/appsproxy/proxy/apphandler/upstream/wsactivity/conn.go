package wsactivity

import "io"

// Wrap returns an io.ReadWriteCloser that observes the byte stream in both
// directions, parses WebSocket frame headers, and invokes onDataFrame once
// per non-control frame (opcodes 0x0..0x7) on either side. Bytes flow through
// unchanged.
//
// The wrapper is intended for the upstream-side io.ReadWriteCloser exposed by
// httputil.ReverseProxy as res.Body for a 101 Switching Protocols response.
// In that role:
//
//   - Read returns bytes flowing from the upstream WebSocket endpoint to the
//     client (unmasked frames per RFC 6455 §5.3).
//   - Write receives bytes flowing from the client to the upstream endpoint
//     (masked frames).
//
// Masking does not affect parsing: only opcode and length fields are inspected,
// both of which are unmasked. Payload bytes are skipped via a counter and never
// buffered, so the wrapper is safe against attacker-controlled payload sizes
// (up to 2^63-1 per the spec).
//
// onDataFrame must be cheap and non-blocking — it runs synchronously on the
// goroutine performing the Read or Write.
func Wrap(rwc io.ReadWriteCloser, onDataFrame FrameCallback) io.ReadWriteCloser {
	return &conn{
		inner:  rwc,
		reader: newFrameParser(onDataFrame),
		writer: newFrameParser(onDataFrame),
	}
}

// conn is a thin passthrough wrapper around an io.ReadWriteCloser that drives
// one frame parser per direction.
type conn struct {
	inner  io.ReadWriteCloser
	reader *frameParser // observes bytes flowing from inner to caller
	writer *frameParser // observes bytes flowing from caller to inner
}

// Read reads from the wrapped conn and feeds the observed bytes to the
// read-side parser before returning them to the caller.
func (c *conn) Read(p []byte) (int, error) {
	n, err := c.inner.Read(p)
	if n > 0 {
		c.reader.Feed(p[:n])
	}
	return n, err
}

// Write writes through to the wrapped conn and feeds the bytes that actually
// reached the underlying writer to the write-side parser. Symmetric with Read
// so a short write does not double-feed if the caller retries with the
// remaining slice.
func (c *conn) Write(p []byte) (int, error) {
	n, err := c.inner.Write(p)
	if n > 0 {
		c.writer.Feed(p[:n])
	}
	return n, err
}

// Close closes the wrapped conn.
func (c *conn) Close() error {
	return c.inner.Close()
}
