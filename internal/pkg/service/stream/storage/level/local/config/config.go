package config

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter"
	encoding "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume"
)

// Config configures the local storage.
type Config struct {
	Volume   volume.Config     `configKey:"volume"`
	Encoding encoding.Config   `configKey:"encoding"`
	Reader   diskreader.Config `configKey:"reader"`
	Writer   diskwriter.Config `configKey:"writer"`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ConfigPatch struct {
	Volume   *volume.ConfigPatch   `json:"volume,omitempty"`
	Encoding *encoding.ConfigPatch `json:"encoding,omitempty"`
}

// NewConfig provides default configuration.
func NewConfig() Config {
	return Config{
		Volume:   volume.NewConfig(),
		Encoding: encoding.NewConfig(),
		Reader:   diskreader.NewConfig(),
		Writer:   diskwriter.NewConfig(),
	}
}
