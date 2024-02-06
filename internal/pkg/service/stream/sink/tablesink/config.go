package tablesink

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage"
)

// Config configures table sink behaviour.
type Config struct {
	Statistics statistics.Config `configKey:"statistics"`
	Storage    storage.Config    `configKey:"storage"`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It is part of the definition.TableSink structure to allow modification of the default configuration.
type ConfigPatch struct {
	Storage *storage.ConfigPatch `json:"storage,omitempty"`
}

func NewConfig() Config {
	return Config{
		Statistics: statistics.NewConfig(),
		Storage:    storage.NewConfig(),
	}
}
