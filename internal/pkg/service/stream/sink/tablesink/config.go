package tablesink

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage"
)

// Config is global configuration of the table sink.
type Config struct{}

// RuntimeConfig is a composed runtime configuration of the table sink.
// Storage configuration is inherited from the service global configuration, see NewRuntimeConfig.
type RuntimeConfig struct {
	Storage storage.Config `json:"storage"`
}

// ConfigPatch provides modification of the RuntimeConfig per sink.
type ConfigPatch struct {
	Storage *storage.ConfigPatch `json:"storage,omitempty"`
}

func NewRuntimeConfig(storageConfig storage.Config) RuntimeConfig {
	return RuntimeConfig{Storage: storageConfig}
}

func NewConfig() Config {
	return Config{}
}
