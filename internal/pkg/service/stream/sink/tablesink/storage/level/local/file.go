package local

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/level/local/writer/diskalloc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/level/local/writer/disksync"
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

func NewFile(cfg Config, fileDir string) File {
	return File{
		Dir:            fileDir,
		Compression:    cfg.Compression.Simplify(),
		DiskSync:       cfg.DiskSync,
		DiskAllocation: cfg.DiskAllocation,
	}
}
