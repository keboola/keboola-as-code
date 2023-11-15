package storage

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/target"
)

// Config contains default configuration for the storage.
// Default settings can be customized in definition.TableSink.
type Config struct {
	Local   local.Config   `configKey:"local"`
	Staging staging.Config `configKey:"staging"`
	Target  target.Config  `configKey:"target"`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It is part of the definition.TableSink structure to allow modification of the default configuration.
type ConfigPatch struct {
	Local   local.ConfigPatch   `json:"local,omitempty"`
	Staging staging.ConfigPatch `json:"staging,omitempty"`
	Target  target.ConfigPatch  `json:"target,omitempty"`
}

// With copies values from the ConfigPatch, if any.
func (c Config) With(v ConfigPatch) Config {
	// Copy values from the ConfigPatch, if any.
	c.Local = c.Local.With(v.Local)
	c.Staging = c.Staging.With(v.Staging)
	c.Target = c.Target.With(v.Target)
	return c
}

func NewConfig() Config {
	return Config{
		Local:   local.NewConfig(),
		Staging: staging.NewConfig(),
		Target:  target.NewConfig(),
	}
}
