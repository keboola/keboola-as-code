package local

import (
	"github.com/c2h5oh/datasize"
	"path/filepath"

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

func (f File) NewSlice(sliceDir string, previousSliceSize datasize.ByteSize) (Slice, error) {
	// Create filename according to the compression type
	filename, err := compression.Filename("slice.csv", f.Compression.Type)
	if err != nil {
		return Slice{}, err
	}

	// Calculated pre-allocated disk space
	var allocatedDiskSpace datasize.ByteSize
	if f.DiskAllocation.Enabled {
		if f.DiskAllocation.SizePercent > 0 && previousSliceSize > 0 {
			allocatedDiskSpace = previousSliceSize
		} else {
			allocatedDiskSpace = (f.DiskAllocation.Size * datasize.ByteSize(f.DiskAllocation.SizePercent)) / 100
		}
	}

	return Slice{
		Dir:                filepath.Join(f.Dir, sliceDir),
		Filename:           filename,
		Compression:        f.Compression,
		DiskSync:           f.DiskSync,
		AllocatedDiskSpace: allocatedDiskSpace,
	}, nil
}
