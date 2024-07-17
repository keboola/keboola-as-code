package local

import (
	"path/filepath"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/model"
)

func NewFile(path string, c Config) model.File {
	return model.File{
		Dir:        NormalizeDirPath(path),
		Allocation: c.Writer.Allocation,
		Encoding:   c.Encoding,
	}
}

func NewSlice(path string, f model.File) (model.Slice, error) {
	// Create filename according to the compression type
	filename, err := compression.Filename("slice.csv", f.Encoding.Compression.Type)
	if err != nil {
		return model.Slice{}, err
	}

	s := model.Slice{
		Dir:      filepath.Join(f.Dir, NormalizeDirPath(path)),
		Filename: filename,
		Encoding: f.Encoding,
	}

	s.Encoding.Compression = s.Encoding.Compression.Simplify()

	return s, nil
}
