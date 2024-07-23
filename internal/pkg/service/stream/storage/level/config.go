package level

import (
	local "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/config"
	staging "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/config"
	target "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/config"
)

// Config contains default configuration for the storage.
// Default settings can be customized in definition.TableSink.
type Config struct {
	Local   local.Config   `configKey:"local"`
	Staging staging.Config `configKey:"staging"`
	Target  target.Config  `configKey:"target"`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ConfigPatch struct {
	Local   *local.ConfigPatch   `json:"local,omitempty"`
	Staging *staging.ConfigPatch `json:"staging,omitempty"`
	Target  *target.ConfigPatch  `json:"target,omitempty"`
}

func NewConfig() Config {
	return Config{
		Local:   local.NewConfig(),
		Staging: staging.NewConfig(),
		Target:  target.NewConfig(),
	}
}
