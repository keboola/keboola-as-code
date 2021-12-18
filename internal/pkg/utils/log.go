package utils

type Reader struct {
	Reader *bufio.Reader
	Buffer *bytes.Buffer
}

func (r *Reader) Read(p []byte) (n int, err error) {
	return r.Reader.Read(p)
}

func (*Reader) Close() error { return nil }

// Fd fake terminal file descriptor.
func (*Reader) Fd() uintptr {
	return os.Stdin.Fd()
}

func NewBufferReader() *Reader {
	var buffer bytes.Buffer
	return &Reader{bufio.NewReader(&buffer), &buffer}
}
