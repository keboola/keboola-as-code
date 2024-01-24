package tablesink

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
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

// With copies values from the ConfigPatch, if any.
func (c Config) With(v ConfigPatch) Config {
	if v.Storage != nil {
		c.Storage = c.Storage.With(*v.Storage)
	}
	return c
}
