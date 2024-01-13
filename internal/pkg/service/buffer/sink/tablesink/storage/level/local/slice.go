package local

import (
	"path/filepath"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/disksync"
)

type Slice struct {
	// Dir defines slice directory in the data volume.
	// The Dir also contains a lock and other auxiliary files.
	Dir string `json:"dir" validate:"required"`
	// Filename of the Slice data file, in the Dir.
	Filename string `json:"filename" validate:"required"`
	// IsEmpty is set if the upload was skipped because we did not receive any data.
	IsEmpty bool `json:"isEmpty,omitempty"`
	// Compression of the local file.
	Compression compression.Config `json:"compression"`
	// DiskSync writer configuration.
	DiskSync disksync.Config `json:"diskSync"`
	// AllocatedDiskSpace defines the disk size that is pre-allocated when creating the slice.
	AllocatedDiskSpace datasize.ByteSize `json:"allocatedDiskSpace"`
}

func (f File) NewSlice(sliceDir string, prevSliceSize datasize.ByteSize) (Slice, error) {
	// Create filename according to the compression type
	filename, err := compression.Filename("slice.csv", f.Compression.Type)
	if err != nil {
		return Slice{}, err
	}

	return Slice{
		Dir:                filepath.Join(f.Dir, sliceDir),
		Filename:           filename,
		Compression:        f.Compression,
		DiskSync:           f.DiskSync,
		AllocatedDiskSpace: f.DiskAllocation.ForNextSlice(prevSliceSize),
	}, nil
}
