package config

import (
	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/encoder"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync"
)

// Config configures the local writer.
type Config struct {
	Encoder               encoder.Config     `json:"encoder" configKey:"encoder"`
	InputBuffer           datasize.ByteSize  `json:"inputBuffer" configKey:"inputBuffer" configUsage:"Max size of the buffer before compression, if compression is enabled. 0 = disabled" validate:"maxBytes=16MB"`
	MaxChunkSize          datasize.ByteSize  `json:"maxChunkSize" configKey:"maxChunkSize" configUsage:"Max size of a chunk sent over the network to a disk writer node." validate:"required,minBytes=64kB,maxBytes=1MB"`
	FailedChunksThreshold int                `json:"failedChunksThreshold" configKey:"failedChunksThreshold" configUsage:"If the defined number of chunks cannot be sent, the pipeline is marked as not ready." validate:"required,min=1,max=100"`
	Compression           compression.Config `json:"compression" configKey:"compression"`
	Sync                  writesync.Config   `json:"sync" configKey:"sync"`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ConfigPatch struct {
	Encoder               *encoder.ConfigPatch     `json:"encoder,omitempty"`
	InputBuffer           *datasize.ByteSize       `json:"inputBuffer,omitempty"`
	MaxChunkSize          *datasize.ByteSize       `json:"maxChunkSize,omitempty"`
	FailedChunksThreshold *int                     `json:"failedChunksThreshold,omitempty"`
	Compression           *compression.ConfigPatch `json:"compression,omitempty"`
	Sync                  *writesync.ConfigPatch   `json:"sync,omitempty"`
}

func NewConfig() Config {
	return Config{
		Encoder:               encoder.NewConfig(),
		InputBuffer:           2 * datasize.MB,
		Compression:           compression.NewConfig(),
		Sync:                  writesync.NewConfig(),
		MaxChunkSize:          512 * datasize.KB,
		FailedChunksThreshold: 3,
	}
}
