package volume

import (
	"io"
	"os"
)

const (
	sliceFileFlags = os.O_CREATE | os.O_WRONLY | os.O_APPEND
	sliceFilePerm  = 0o640
)

// File contains all *os.File methods used by this package.
// This makes it possible to use a custom file implementation in the tests, see FileOpener.
type File interface {
	io.Writer
	Fd() uintptr
	Stat() (os.FileInfo, error)
	Sync() error
	Close() error
}

// FileOpener opens the File for writing.
type FileOpener func(filePath string) (File, error)

func DefaultFileOpener(filePath string) (File, error) {
	return os.OpenFile(filePath, sliceFileFlags, sliceFilePerm)
}
