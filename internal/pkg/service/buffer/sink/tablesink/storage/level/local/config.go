package local

import (
	"github.com/c2h5oh/datasize"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/disksync"
)

// Config contains default configuration for the local storage.
type Config struct {
	// Compression of the local file.
	Compression compression.Config `configKey:"compression"`
	// VolumesAssignment configures assignment of pod volumes to the File.
	VolumesAssignment VolumesAssignment `configKey:"volumesAssignment"`
	// DiskSync configures the synchronization of the in-memory copy of written data to disk or OS disk cache.
	DiskSync disksync.Config `configKey:"diskSync"`
	// DiskAllocation configures allocation of the disk space for file slices.
	DiskAllocation DiskAllocation `configKey:"diskAllocation"`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It is part of the definition.TableSink structure to allow modification of the default configuration.
type ConfigPatch struct {
	// Compression of the local and staging file.
	Compression *compression.Config `json:"compression,omitempty"`
	// VolumesAssignment configures assignment of pod volumes to the File.
	VolumesAssignment *VolumesAssignment `json:"volumesAssignment,omitempty"`
	// DiskSync configures the synchronization of the in-memory copy of written data to disk or OS disk cache.
	DiskSync *disksync.Config `json:"diskSync,omitempty"`
	// DiskAllocation configures allocation of the disk space for file slices.
	DiskAllocation *DiskAllocation `json:"diskAllocation,omitempty"`
}

// DiskAllocation configures allocation of the disk space for file slices.
// Read more in the writer/allocate package.
type DiskAllocation struct {
	// Enabled enables disk space allocation.
	Enabled bool `json:"enabled" configKey:"enabled" configUsage:"Allocate disk space for each slice."`
	// Size of the first slice in a sink, or the size of each slice if DynamicSize is disabled.
	Size datasize.ByteSize `json:"size" configKey:"size" configUsage:"Size of allocated disk space for a slice."`
	// SizePercent enables dynamic size of allocated disk space.
	// If enabled, the size of the slice will be % from the previous slice size.
	// The size of the first slice in a sink will be Size.
	SizePercent int `json:"sizePercent" configKey:"sizePercent" validate:"min=0,max=200" configUsage:"Allocate disk space as % from the previous slice size."`
}

// With copies values from the ConfigPatch, if any.
func (c Config) With(v ConfigPatch) Config {
	if v.Compression != nil {
		c.Compression = *v.Compression
	}
	if v.VolumesAssignment != nil {
		c.VolumesAssignment = *v.VolumesAssignment
	}
	if v.DiskSync != nil {
		c.DiskSync = *v.DiskSync
	}
	if v.DiskAllocation != nil {
		c.DiskAllocation = *v.DiskAllocation
	}
	return c
}

func NewConfig() Config {
	return Config{
		Compression:       compression.DefaultConfig(),
		VolumesAssignment: defaultVolumesAssigment(),
		DiskSync:          disksync.DefaultConfig(),
		DiskAllocation:    defaultDiskAllocation(),
	}
}

func defaultVolumesAssigment() VolumesAssignment {
	return VolumesAssignment{
		PerPod:         1,
		PreferredTypes: []string{"default"},
	}
}

func defaultDiskAllocation() DiskAllocation {
	return DiskAllocation{
		Enabled:     true,
		SizePercent: 110,
		Size:        100 * datasize.MB,
	}
}
