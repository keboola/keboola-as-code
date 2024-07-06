package volume

import (
	"io"
	"os"
)

const (
	sliceFileFlags = os.O_RDONLY
	sliceFilePerm  = 0 // must exists
)

// File contains all *os.File methods used by this package.
// This makes it possible to use a custom file implementation in the tests, see FileOpener.
type File interface {
	io.ReadCloser
}

// FileOpener opens the File for reading.
type FileOpener func(filePath string) (File, error)

func DefaultFileOpener(filePath string) (File, error) {
	return os.OpenFile(filePath, sliceFileFlags, sliceFilePerm)
}
