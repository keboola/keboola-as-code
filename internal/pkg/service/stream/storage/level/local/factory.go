package local

import (
	"path/filepath"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/model"
)

func NewFile(path string, c Config) model.File {
	return model.File{
		Dir:            NormalizeDirPath(path),
		Compression:    c.Encoding.Compression.Simplify(),
		DiskSync:       c.Volume.Sync,
		DiskAllocation: c.Volume.Allocation,
	}
}

func NewSlice(path string, f model.File) (model.Slice, error) {
	// Create filename according to the compression type
	filename, err := compression.Filename("slice.csv", f.Compression.Type)
	if err != nil {
		return model.Slice{}, err
	}

	return model.Slice{
		Dir:         filepath.Join(f.Dir, NormalizeDirPath(path)),
		Filename:    filename,
		Compression: f.Compression,
		DiskSync:    f.DiskSync,
	}, nil
}
