package diskwriter

import (
	"io"
	"os"
)

const (
	sliceFileFlags = os.O_CREATE | os.O_WRONLY
	sliceFilePerm  = 0o640
)

// File contains all *os.File methods used by this package.
// This makes it possible to use a custom file implementation in the tests, see FileOpener.
type File interface {
	io.Writer
	Fd() uintptr
	Seek(offset int64, whence int) (ret int64, err error)
	Truncate(size int64) error
	Stat() (os.FileInfo, error)
	Sync() error
	Close() error
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
	f, err := os.OpenFile(filePath, sliceFileFlags, sliceFilePerm)
	if err != nil {
		return nil, err
	}

	// Windows does not support truncate of file in `os.O_APPEND` file mode
	_, err = f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	return f, nil
}

type fileOpenerFn struct {
	Fn func(filePath string) (File, error)
}

func (o *fileOpenerFn) OpenFile(filePath string) (File, error) {
	return o.Fn(filePath)
}
