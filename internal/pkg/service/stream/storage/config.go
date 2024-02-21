package storage

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

// Config contains default configuration for the storage.
// Default settings can be customized in definition.TableSink.
type Config struct {
	Statistics statistics.Config `configKey:"statistics"`
	Level      level.Config      `configKey:"level"`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ConfigPatch struct {
	Level *level.ConfigPatch `json:"level,omitempty"`
}

func NewConfig() Config {
	return Config{
		Statistics: statistics.NewConfig(),
		Level:      level.NewConfig(),
	}
}
