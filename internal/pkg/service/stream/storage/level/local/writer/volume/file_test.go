package volume

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// testFile provides implementation of the File interface for tests.
type testFile struct {
	file       *os.File
	CloseError error
}

func newTestFile(t *testing.T, filePath string) *testFile {
	t.Helper()
	file, err := os.OpenFile(filePath, sliceFileFlags, sliceFilePerm)
	require.NoError(t, err)
	return &testFile{file: file}
}

func (f *testFile) Write(p []byte) (int, error) {
	return f.file.Write(p)
}

func (f *testFile) WriteString(s string) (int, error) {
	return f.file.WriteString(s)
}

func (f *testFile) Fd() uintptr {
	return f.file.Fd()
}

func (f *testFile) Stat() (os.FileInfo, error) {
	return f.file.Stat()
}

func (f *testFile) Sync() error {
	return f.file.Sync()
}

func (f *testFile) Close() error {
	err := f.file.Close()
	if f.CloseError != nil {
		return f.CloseError
	}
	return err
}
