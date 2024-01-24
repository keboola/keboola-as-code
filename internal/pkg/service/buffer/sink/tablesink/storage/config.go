package storage

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/target"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/volume/assignment"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/volume/registration"
)

// Config contains default configuration for the storage.
// Default settings can be customized in definition.TableSink.
type Config struct {
	VolumeAssignment   assignment.Config   `configKey:"volumeAssignment"`
	VolumeRegistration registration.Config `configKey:"volumeRegistration"`
	Local              local.Config        `configKey:"local"`
	Staging            staging.Config      `configKey:"staging"`
	Target             target.Config       `configKey:"target"`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It is part of the definition.TableSink structure to allow modification of the default configuration.
type ConfigPatch struct {
	VolumeAssignment   *assignment.Config   `json:"volumeAssignment,omitempty"`
	Local              *local.ConfigPatch   `json:"local,omitempty"`
	Staging            *staging.ConfigPatch `json:"staging,omitempty"`
	Target             *target.ConfigPatch  `json:"target,omitempty"`
}

// With copies values from the ConfigPatch, if any.
func (c Config) With(v *ConfigPatch) Config {
	// Copy values from the ConfigPatch, if any.
	if v != nil {
		if v.VolumeAssignment != nil {
			c.VolumeAssignment = *v.VolumeAssignment
		}
		if v.VolumeRegistration != nil {
			c.VolumeRegistration = *v.VolumeRegistration
		}
		if v.Local != nil {
			c.Local = c.Local.With(*v.Local)
		}
		if v.Staging != nil {
			c.Staging = c.Staging.With(*v.Staging)
		}
		if v.Target != nil {
			c.Target = c.Target.With(*v.Target)
		}
	}
	return c
}

func NewConfig() Config {
	return Config{
		VolumeAssignment:   assignment.NewConfig(),
		VolumeRegistration: registration.NewConfig(),
		Local:              local.NewConfig(),
		Staging:            staging.NewConfig(),
		Target:             target.NewConfig(),
	}
}
