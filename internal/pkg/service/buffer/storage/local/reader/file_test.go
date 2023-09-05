package reader

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"io"
)

// testFile provides implementation of the File interface for tests.
type testFile struct {
	reader     io.Reader
	ReadError  error
	CloseError error
}

func newTestFile(r io.Reader) *testFile {
	return &testFile{reader: r}
}

func (r *testFile) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	if r.ReadError != nil && (n == 0 || errors.Is(err, io.EOF)) {
		return 0, r.ReadError
	}
	return n, err
}

func (r *testFile) Close() error {
	return r.CloseError
}
