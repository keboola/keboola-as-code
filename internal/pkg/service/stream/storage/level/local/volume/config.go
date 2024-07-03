package volume

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/assignment"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/registration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/writernode/diskalloc"
)

// Config configures assignment of pod volumes to a File.
type Config struct {
	Assignment   assignment.Config   `configKey:"assignment"`
	Registration registration.Config `configKey:"registration"`
	// Sync configures the synchronization of the in-memory copy of written data to disk or OS disk cache.
	Sync disksync.Config `configKey:"sync"`
	// Allocation configures allocation of the disk space for file slices.
	Allocation diskalloc.Config `configKey:"allocation"`
}

type ConfigPatch struct {
	Assignment *assignment.ConfigPatch `json:"assignment,omitempty"`
	// Sync configures the synchronization of the in-memory copy of written data to disk or OS disk cache.
	Sync *disksync.ConfigPatch `json:"sync,omitempty"`
	// Allocation configures allocation of the disk space for file slices.
	Allocation *diskalloc.ConfigPatch `json:"allocation,omitempty"`
}

// NewConfig provides default configuration.
func NewConfig() Config {
	return Config{
		Assignment:   assignment.NewConfig(),
		Registration: registration.NewConfig(),
		Sync:         disksync.NewConfig(),
		Allocation:   diskalloc.NewConfig(),
	}
}
