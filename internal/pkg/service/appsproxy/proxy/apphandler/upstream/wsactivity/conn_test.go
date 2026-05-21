package wsactivity

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// fakeRWC is a minimal io.ReadWriteCloser used to drive the wrapper from
// either direction. Read consumes from rBuf; Write appends to wBuf.
type fakeRWC struct {
	rBuf       *bytes.Buffer
	wBuf       *bytes.Buffer
	closed     bool
	writeErr   error // injected error for Write
	readErr    error // injected error after rBuf drained
	shortWrite bool  // if true, Write returns half of len(p) and io.ErrShortWrite
}

func (f *fakeRWC) Read(p []byte) (int, error) {
	if f.rBuf.Len() == 0 {
		if f.readErr != nil {
			return 0, f.readErr
		}
		return 0, io.EOF
	}
	return f.rBuf.Read(p)
}

func (f *fakeRWC) Write(p []byte) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	if f.shortWrite {
		n := len(p) / 2
		_, _ = f.wBuf.Write(p[:n])
		return n, io.ErrShortWrite
	}
	return f.wBuf.Write(p)
}

func (f *fakeRWC) Close() error {
	f.closed = true
	return nil
}

func newFakeRWC(readData []byte) *fakeRWC {
	return &fakeRWC{rBuf: bytes.NewBuffer(readData), wBuf: &bytes.Buffer{}}
}

func TestWrap_Read_CountsServerToClientFrames(t *testing.T) {
	t.Parallel()
	// Two unmasked text frames from server (Read side).
	stream := append(
		buildFrame(t, 0x1, true, 4, false),
		buildFrame(t, 0x2, true, 8, false)...,
	)
	rwc := newFakeRWC(stream)

	count := 0
	c := Wrap(rwc, func() { count++ })

	out, err := io.ReadAll(c)
	require.NoError(t, err)
	assert.Equal(t, stream, out, "pass-through must be byte-identical")
	assert.Equal(t, 2, count)
}

func TestWrap_Write_CountsClientToServerFrames(t *testing.T) {
	t.Parallel()
	// One masked text frame the client would write toward the server.
	frame := buildFrame(t, 0x1, true, 6, true)
	rwc := newFakeRWC(nil)

	count := 0
	c := Wrap(rwc, func() { count++ })

	n, err := c.Write(frame)
	require.NoError(t, err)
	assert.Equal(t, len(frame), n)
	assert.Equal(t, frame, rwc.wBuf.Bytes(), "pass-through must be byte-identical")
	assert.Equal(t, 1, count)
}

func TestWrap_ReadAndWrite_BothDirectionsIndependent(t *testing.T) {
	t.Parallel()
	// Read side: one text frame. Write side: one text frame + one ping (ignored).
	readStream := buildFrame(t, 0x1, true, 5, false)
	writeStream := append(
		buildFrame(t, 0x1, true, 3, true),
		buildFrame(t, 0x9, true, 0, true)...,
	)
	rwc := newFakeRWC(readStream)

	count := 0
	c := Wrap(rwc, func() { count++ })

	// Read first.
	got, err := io.ReadAll(c)
	require.NoError(t, err)
	assert.Equal(t, readStream, got)

	// Then write.
	n, err := c.Write(writeStream)
	require.NoError(t, err)
	assert.Equal(t, len(writeStream), n)

	// Expect 1 (from read text frame) + 1 (from write text frame) = 2.
	assert.Equal(t, 2, count)
}

func TestWrap_OnlyControlFrames_NoCallbacks(t *testing.T) {
	t.Parallel()
	// Idle WS pattern: only ping/pong in either direction.
	pings := append(
		buildFrame(t, 0x9, true, 0, false),
		buildFrame(t, 0xA, true, 0, false)...,
	)
	rwc := newFakeRWC(pings)

	count := 0
	c := Wrap(rwc, func() { count++ })

	_, err := io.ReadAll(c)
	require.NoError(t, err)
	_, err = c.Write(pings)
	require.NoError(t, err)

	assert.Equal(t, 0, count, "control-only traffic must not trigger notify")
}

func TestWrap_PassThroughGolden(t *testing.T) {
	t.Parallel()
	// Random-ish stream of mixed frames — the wrapper must not mutate any byte.
	parts := [][]byte{
		buildFrame(t, 0x1, true, 7, true),
		buildFrame(t, 0x9, true, 0, false),
		buildFrame(t, 0x2, false, 200, true), // 16-bit len
		buildFrame(t, 0x0, true, 100, true),
		buildFrame(t, 0x8, true, 2, false), // close
	}
	total := 0
	for _, p := range parts {
		total += len(p)
	}
	stream := make([]byte, 0, total)
	for _, p := range parts {
		stream = append(stream, p...)
	}

	rwc := newFakeRWC(stream)
	c := Wrap(rwc, func() {})

	got, err := io.ReadAll(c)
	require.NoError(t, err)
	assert.Equal(t, stream, got)
}

func TestWrap_Close_DelegatesToInner(t *testing.T) {
	t.Parallel()
	rwc := newFakeRWC(nil)
	c := Wrap(rwc, func() {})
	require.NoError(t, c.Close())
	assert.True(t, rwc.closed)
}

func TestWrap_ShortWrite_DoesNotDoubleFeed(t *testing.T) {
	t.Parallel()
	// Inner Write returns half of p with io.ErrShortWrite. The parser must
	// only see the bytes that actually reached the inner writer, so that the
	// caller's retry with the unwritten tail doesn't cause double-counting.
	frame := buildFrame(t, 0x1, true, 10, true) // total ~20 bytes
	rwc := &fakeRWC{
		rBuf:       &bytes.Buffer{},
		wBuf:       &bytes.Buffer{},
		shortWrite: true,
	}

	count := 0
	c := Wrap(rwc, func() { count++ })

	n, err := c.Write(frame)
	require.ErrorIs(t, err, io.ErrShortWrite)
	assert.Equal(t, len(frame)/2, n)

	// The first half includes only the partial header; the parser shouldn't
	// have fired a callback yet (it needs the full header).
	// Now simulate the caller retrying with the unwritten tail (no shortWrite
	// this time so it succeeds completely).
	rwc.shortWrite = false
	rest := frame[n:]
	n2, err := c.Write(rest)
	require.NoError(t, err)
	assert.Equal(t, len(rest), n2)

	assert.Equal(t, 1, count, "exactly one callback even after a short write + retry")
}

func TestWrap_WriteErrorDoesNotFeed(t *testing.T) {
	t.Parallel()
	rwc := &fakeRWC{rBuf: &bytes.Buffer{}, wBuf: &bytes.Buffer{}, writeErr: errors.New("boom")}

	count := 0
	c := Wrap(rwc, func() { count++ })
	frame := buildFrame(t, 0x1, true, 5, true)

	_, err := c.Write(frame)
	require.Error(t, err)
	assert.Equal(t, 0, count, "no bytes reached the wire — parser must not advance")
}
