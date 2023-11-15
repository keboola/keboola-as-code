package local

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local/writer/disksync"
)

type File struct {
	// Dir defines file directory in the data volume.
	Dir string `json:"dir" validate:"required"`
	// Compression of the local file.
	Compression compression.Config `json:"compression"`
	// Volumes configures assignment of pod volumes to the File.
	VolumesAssignment VolumesAssignment `json:"volumesAssignment"`
	// DiskSync configures the synchronization of the in-memory copy of written data to disk or OS disk cache.
	DiskSync disksync.Config `json:"diskSync"`
	// DiskAllocation configures pre-allocation of the disk space for file slices.
	DiskAllocation DiskAllocation `json:"diskAllocation"`
}
