package encoding

import (
	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
)

// Config configures the local writer.
type Config struct {
	Concurrency  int                `configKey:"concurrency" configUsage:"Concurrency of the format writer for the specified file type. 0 = auto = num of CPU cores" validate:"min=0,max=256"`
	InputBuffer  datasize.ByteSize  `configKey:"inputBuffer" configUsage:"Max size of the buffer before compression, if compression is enabled. 0 = disabled" validate:"maxBytes=16MB"`
	OutputBuffer datasize.ByteSize  `configKey:"outputBuffer" configUsage:"Max size of the buffer before the output. 0 = disabled" validate:"maxBytes=16MB"`
	Compression  compression.Config `configKey:"compression"`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ConfigPatch struct {
	Concurrency *int                     `json:"concurrency,omitempty"`
	InputBuffer *datasize.ByteSize       `json:"inputBuffer,omitempty"`
	FileBuffer  *datasize.ByteSize       `json:"outputBuffer,omitempty"`
	Compression *compression.ConfigPatch `json:"compression,omitempty"`
}

func NewConfig() Config {
	return Config{
		Concurrency:  0, // 0 = auto = CPU cores
		InputBuffer:  1 * datasize.MB,
		OutputBuffer: 1 * datasize.MB,
		Compression:  compression.NewConfig(),
	}
}
