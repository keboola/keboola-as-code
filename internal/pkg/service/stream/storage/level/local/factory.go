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
	}
}

func NewSlice(path string, f model.File, compressionCfg compression.Config) (model.Slice, error) {
	// Create filename according to the compression type
	filename, err := compression.Filename("slice.csv", compressionCfg.Type)
	if err != nil {
		return model.Slice{}, err
	}

	s := model.Slice{
		Dir:      filepath.Join(f.Dir, NormalizeDirPath(path)),
		Filename: filename,
	}

	return s, nil
}
