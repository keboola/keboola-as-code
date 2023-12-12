// Package reader provides reading of tabular data from local storage for upload to staging storage.
// Data may be compressed on-tly-fly according to the configuration.
// Regarding creating a reader, see:
//   - The New function.
//   - The "volume" package and the volume.NewReaderFor method in the package.
package reader

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local/reader/readchain"
	"io"
	"os"
)

type Reader interface {
	io.ReadCloser

	SliceKey() storage.SliceKey

	// UnwrapFile returns underlying file, if it is the only element in the chain.
	//
	// This is preferred way, it enables internal Go optimization and "zero CPU copy" to be used,
	// read more about "sendfile" syscall for details.
	//
	// The Close methods should always be called on the Chain, not directly on the File,
	// because Chain may contain multiple closers, even if there is only one reader.
	UnwrapFile() (f *os.File, ok bool)

	// DirPath is absolute path to the slice directory. It contains slice file and optionally an auxiliary files.
	DirPath() string
	// FilePath is absolute path to the slice file.
	FilePath() string
}

type readChain = readchain.Chain

type reader struct {
	*readChain
	sliceKey storage.SliceKey
	dirPath  string
	filePath string
}

func (r *reader) SliceKey() storage.SliceKey {
	return r.sliceKey
}

func (r *reader) DirPath() string {
	return r.dirPath
}

func (r *reader) FilePath() string {
	return r.filePath
}

func New(chain *readchain.Chain, sliceKey storage.SliceKey, dirPath, filePath string) Reader {
	return &reader{readChain: chain, sliceKey: sliceKey, dirPath: dirPath, filePath: filePath}
}
