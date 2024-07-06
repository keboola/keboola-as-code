package test

import (
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// ReaderTestFile provides implementation of the File interface for tests.
type ReaderTestFile struct {
	reader     io.Reader
	ReadError  error
	CloseError error
}

func NewReaderTestFile(r io.Reader) *ReaderTestFile {
	return &ReaderTestFile{reader: r}
}

func (r *ReaderTestFile) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	if r.ReadError != nil && (n == 0 || errors.Is(err, io.EOF)) {
		return 0, r.ReadError
	}
	return n, err
}

func (r *ReaderTestFile) Close() error {
	return r.CloseError
}
