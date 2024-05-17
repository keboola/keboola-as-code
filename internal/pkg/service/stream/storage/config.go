package storage

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/cleanup"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

// Config contains global configuration for the storage.
type Config struct {
	VolumesPath string            `configKey:"volumesPath" configUsage:"Mounted volumes path, each volume is in \"{type}/{label}\" subdir." validate:"required"`
	Statistics  statistics.Config `configKey:"statistics"`
	Cleanup     cleanup.Config    `configKey:"cleanup"`
	Level       level.Config      `configKey:"level"`
}

type ConfigPatch struct {
	Level *level.ConfigPatch `json:"level,omitempty"`
}

func NewConfig() Config {
	return Config{
		Statistics: statistics.NewConfig(),
		Cleanup:    cleanup.NewConfig(),
		Level:      level.NewConfig(),
	}
}
