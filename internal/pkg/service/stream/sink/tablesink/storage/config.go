package storage

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/level/target"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/volume/assignment"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/volume/registration"
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
	VolumeAssignment *assignment.ConfigPatch `json:"volumeAssignment,omitempty"`
	Local            *local.ConfigPatch      `json:"local,omitempty"`
	Staging          *staging.ConfigPatch    `json:"staging,omitempty"`
	Target           *target.ConfigPatch     `json:"target,omitempty"`
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
