package ioutil

import (
	"bufio"
	"bytes"
	"os"
)

// Reader is a simple buffer reader for testing.
// It implements these interfaces:
// - io.Reader
// - io.ReadCloser
// - io.Closer
// - terminal.FileReader.
type Reader struct {
	Reader *bufio.Reader
	Buffer *bytes.Buffer
}

func NewBufferedReader() *Reader {
	var buffer bytes.Buffer
	return &Reader{bufio.NewReader(&buffer), &buffer}
}

func (r *Reader) Read(p []byte) (n int, err error) {
	return r.Reader.Read(p)
}

func (*Reader) Close() error { return nil }

// Fd fake terminal file descriptor.
func (*Reader) Fd() uintptr {
	return os.Stdin.Fd()
}
