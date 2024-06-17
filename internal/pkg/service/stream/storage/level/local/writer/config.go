package writer

import (
	"time"

	"github.com/c2h5oh/datasize"
)

// Config configures the local writer.
type Config struct {
	Concurrency int               `configKey:"concurrency" configUsage:"Concurrency of the writer for the specified file type. 0 = auto = num of CPU cores" validate:"min=0,max=256"`
	InputBuffer datasize.ByteSize `configKey:"inputBuffer" configUsage:"Max size of the buffer before compression, if compression is enabled. 0 = disabled" validate:"maxBytes=16MB"`
	FileBuffer  datasize.ByteSize `configKey:"fileBuffer" configUsage:"Max size of the buffer before the output file. 0 = disabled" validate:"maxBytes=16MB"`
	Statistics  StatisticsConfig  `configKey:"statistics"`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ConfigPatch struct {
	Concurrency *int                   `json:"concurrency,omitempty"`
	InputBuffer *datasize.ByteSize     `json:"inputBuffer,omitempty"`
	FileBuffer  *datasize.ByteSize     `json:"fileBuffer,omitempty"`
	Statistics  *StatisticsConfigPatch `json:"statistics,omitempty"`
}

type StatisticsConfig struct {
	// DiskSyncInterval of in-memory statistics to a disk file, for an outage in the future.
	// The value 0 means no periodic syncs. Values are always synced up on writer close.
	// See count.Counter and size.Meter for details.
	DiskSyncInterval time.Duration `configKey:"diskSyncInterval" configUsage:"Sync interval of in-memory statistics to disk, as a backup. 0 = disabled." validate:"maxDuration=1m"`
}

// StatisticsConfigPatch is same as the StatisticsConfig, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type StatisticsConfigPatch struct {
	DiskSyncInterval *time.Duration `json:"diskSyncInterval,omitempty"`
}

// NewConfig provides default configuration.
func NewConfig() Config {
	return Config{
		Concurrency: 0, // 0 = auto = CPU cores
		InputBuffer: 1 * datasize.MB,
		FileBuffer:  1 * datasize.MB,
		Statistics: StatisticsConfig{
			DiskSyncInterval: time.Second,
		},
	}
}
