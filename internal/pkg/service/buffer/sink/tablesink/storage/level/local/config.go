package local

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/allocate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/disksync"
)

// Config contains default configuration for the local storage.
type Config struct {
	// Compression of the local file.
	Compression compression.Config `configKey:"compression"`
	// DiskSync configures the synchronization of the in-memory copy of written data to disk or OS disk cache.
	DiskSync disksync.Config `configKey:"diskSync"`
	// DiskAllocation configures allocation of the disk space for file slices.
	DiskAllocation allocate.Config `configKey:"diskAllocation"`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It is part of the definition.TableSink structure to allow modification of the default configuration.
type ConfigPatch struct {
	// Compression of the local and staging file.
	Compression *compression.Config `json:"compression,omitempty"`
	// DiskSync configures the synchronization of the in-memory copy of written data to disk or OS disk cache.
	DiskSync *disksync.Config `json:"diskSync,omitempty"`
	// DiskAllocation configures allocation of the disk space for file slices.
	DiskAllocation *allocate.Config `json:"diskAllocation,omitempty"`
}

// With copies values from the ConfigPatch, if any.
func (c Config) With(v ConfigPatch) Config {
	if v.Compression != nil {
		c.Compression = *v.Compression
	}
	if v.DiskSync != nil {
		c.DiskSync = *v.DiskSync
	}
	if v.DiskAllocation != nil {
		c.DiskAllocation = *v.DiskAllocation
	}
	return c
}

// NewConfig provides default configuration.
func NewConfig() Config {
	return Config{
		Compression:    compression.NewConfig(),
		DiskSync:       disksync.NewConfig(),
		DiskAllocation: allocate.NewConfig(),
	}
}
