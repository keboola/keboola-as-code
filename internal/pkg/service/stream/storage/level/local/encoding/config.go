package encoding

import (
	"github.com/c2h5oh/datasize"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/encoder"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync"
)

// Config configures the local writer.
type Config struct {
	Encoder      encoder.Config     `configKey:"encoder"`
	InputBuffer  datasize.ByteSize  `configKey:"inputBuffer" configUsage:"Max size of the buffer before compression, if compression is enabled. 0 = disabled" validate:"maxBytes=16MB"`
	OutputBuffer datasize.ByteSize  `configKey:"outputBuffer" configUsage:"Max size of the buffer before the output. 0 = disabled" validate:"maxBytes=16MB"`
	Compression  compression.Config `configKey:"compression"`
	// OutputOpener TODO
	// A custom implementation can be useful for tests.
	OutputOpener OutputOpener
	// SyncerFactory creates writesync.Syncer.
	// A custom implementation can be useful for tests.
	SyncerFactory writesync.SyncerFactory
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ConfigPatch struct {
	Encoder     *encoder.ConfigPatch     `json:"encoder,omitempty"`
	InputBuffer *datasize.ByteSize       `json:"inputBuffer,omitempty"`
	FileBuffer  *datasize.ByteSize       `json:"outputBuffer,omitempty"`
	Compression *compression.ConfigPatch `json:"compression,omitempty"`
}

func NewConfig() Config {
	return Config{
		Encoder:       encoder.NewConfig(),
		InputBuffer:   1 * datasize.MB,
		OutputBuffer:  1 * datasize.MB,
		Compression:   compression.NewConfig(),
		OutputOpener:  DefaultFileOpener,
		SyncerFactory: writesync.NewSyncer,
	}
}
