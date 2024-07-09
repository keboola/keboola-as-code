package model

import (
	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/source/writesync"
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
	DiskSync writesync.Config `json:"diskSync"`
	// AllocatedDiskSpace defines the disk size that is pre-allocated when creating the slice.
	AllocatedDiskSpace datasize.ByteSize `json:"allocatedDiskSpace"`
}
