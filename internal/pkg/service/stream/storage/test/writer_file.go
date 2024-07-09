package test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// WriterTestFile provides implementation of the File interface for tests.
type WriterTestFile struct {
	file       *os.File
	CloseError error
}

func NewWriterTestFile(t *testing.T, filePath string) *WriterTestFile {
	t.Helper()
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	require.NoError(t, err)
	return &WriterTestFile{file: file}
}

func (f *WriterTestFile) Write(p []byte) (int, error) {
	return f.file.Write(p)
}

func (f *WriterTestFile) WriteString(s string) (int, error) {
	return f.file.WriteString(s)
}

func (f *WriterTestFile) Fd() uintptr {
	return f.file.Fd()
}

func (f *WriterTestFile) Stat() (os.FileInfo, error) {
	return f.file.Stat()
}

func (f *WriterTestFile) Sync() error {
	return f.file.Sync()
}

func (f *WriterTestFile) Close() error {
	err := f.file.Close()
	if f.CloseError != nil {
		return f.CloseError
	}
	return err
}
