package diskreader

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
type FileOpener interface {
	OpenFile(filePath string) (File, error)
}

func FileOpenerFn(fn func(filePath string) (File, error)) FileOpener {
	return &fileOpenerFn{Fn: fn}
}

type DefaultFileOpener struct{}

func (DefaultFileOpener) OpenFile(filePath string) (File, error) {
	return os.OpenFile(filePath, sliceFileFlags, sliceFilePerm)
}

type fileOpenerFn struct {
	Fn func(filePath string) (File, error)
}

func (o *fileOpenerFn) OpenFile(filePath string) (File, error) {
	return o.Fn(filePath)
}
