package local

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume"
)

// Config configures the local storage.
type Config struct {
	Volume volume.Config `configKey:"volume"`
	// Compression of the local file.
	Compression compression.Config `configKey:"compression"`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ConfigPatch struct {
	Volume *volume.ConfigPatch `json:"volume,omitempty"`
	// Compression of the local and staging file.
	Compression *compression.ConfigPatch `json:"compression,omitempty"`
}

// NewConfig provides default configuration.
func NewConfig() Config {
	return Config{
		Volume:      volume.NewConfig(),
		Compression: compression.NewConfig(),
	}
}
