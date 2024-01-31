package local

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/diskalloc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/disksync"
)

// Config configures the local storage.
type Config struct {
	// Compression of the local file.
	Compression compression.Config `configKey:"compression"`
	// DiskSync configures the synchronization of the in-memory copy of written data to disk or OS disk cache.
	DiskSync disksync.Config `configKey:"diskSync"`
	// DiskAllocation configures allocation of the disk space for file slices.
	DiskAllocation diskalloc.Config `configKey:"diskAllocation"`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It is part of the definition.TableSink structure to allow modification of the default configuration.
type ConfigPatch struct {
	// Compression of the local and staging file.
	Compression *compression.ConfigPatch `json:"compression,omitempty"`
	// DiskSync configures the synchronization of the in-memory copy of written data to disk or OS disk cache.
	DiskSync *disksync.ConfigPatch `json:"diskSync,omitempty"`
	// DiskAllocation configures allocation of the disk space for file slices.
	DiskAllocation *diskalloc.ConfigPatch `json:"diskAllocation,omitempty"`
}

// NewConfig provides default configuration.
func NewConfig() Config {
	return Config{
		Compression:    compression.NewConfig(),
		DiskSync:       disksync.NewConfig(),
		DiskAllocation: diskalloc.NewConfig(),
	}
}
