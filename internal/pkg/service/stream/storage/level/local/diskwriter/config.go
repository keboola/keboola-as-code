package diskwriter

import (
	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
)

// Config configures the local writer.
type Config struct {
	Format      encoding.Config   `configKey:"format"`
	InputBuffer datasize.ByteSize `configKey:"inputBuffer" configUsage:"Max size of the buffer before compression, if compression is enabled. 0 = disabled" validate:"maxBytes=16MB"`
	FileBuffer  datasize.ByteSize `configKey:"fileBuffer" configUsage:"Max size of the buffer before the output file. 0 = disabled" validate:"maxBytes=16MB"`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ConfigPatch struct {
	Format      *encoding.ConfigPatch `json:"format,omitempty"`
	InputBuffer *datasize.ByteSize    `json:"inputBuffer,omitempty"`
	FileBuffer  *datasize.ByteSize    `json:"fileBuffer,omitempty"`
}

// NewConfig provides default configuration.
func NewConfig() Config {
	return Config{
		Format:      encoding.NewConfig(),
		InputBuffer: 1 * datasize.MB,
		FileBuffer:  1 * datasize.MB,
	}
}
