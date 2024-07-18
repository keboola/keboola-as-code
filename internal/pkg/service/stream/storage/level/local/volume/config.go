package volume

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/assignment"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/registration"
)

// Config configures assignment of pod volumes to a File.
type Config struct {
	Assignment   assignment.Config   `configKey:"assignment"`
	Registration registration.Config `configKey:"registration"`
}

type ConfigPatch struct {
	Assignment *assignment.ConfigPatch `json:"assignment,omitempty"`
}

// NewConfig provides default configuration.
func NewConfig() Config {
	return Config{
		Assignment:   assignment.NewConfig(),
		Registration: registration.NewConfig(),
	}
}
