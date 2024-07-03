package model

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/writernode/diskalloc"
)

type File struct {
	// Dir defines file directory in the data volume.
	Dir string `json:"dir" validate:"required"`
	// Compression of the local file.
	Compression compression.Config `json:"compression"`
	// DiskSync configures the synchronization of the in-memory copy of written data to disk or OS disk cache.
	DiskSync disksync.Config `json:"diskSync"`
	// DiskAllocation configures pre-allocation of the disk space for file slices.
	DiskAllocation diskalloc.Config `json:"diskAllocation"`
}
