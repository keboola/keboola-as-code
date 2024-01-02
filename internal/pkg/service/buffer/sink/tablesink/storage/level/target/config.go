package target

import (
	"time"

	"github.com/c2h5oh/datasize"
)

// Config contains default configuration for the target storage.
type Config struct {
	Import ImportConfig `configKey:"import"`
}

type ImportConfig struct {
	MinInterval time.Duration     `configKey:"minInterval" configUsage:"Minimal interval between imports." validate:"required,minDuration=30s,maxDuration=30m"`
	Trigger     FileImportTrigger `configKey:"trigger"`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It is part of the definition.TableSink structure to allow modification of the default configuration.
type ConfigPatch struct {
	Import *ImportConfigPatch `json:"import,omitempty"`
}

type ImportConfigPatch struct {
	Trigger *FileImportTrigger `json:"trigger,omitempty"`
}

// FileImportTrigger struct configures conditions for import of the sliced file to the target table.
type FileImportTrigger struct {
	Count    uint64            `json:"count" configKey:"count" configUsage:"Records count." validate:"required,min=1,max=10000000"`
	Size     datasize.ByteSize `json:"size" configKey:"size" configUsage:"Records size." validate:"required,minBytes=100B,maxBytes=500MB"`
	Interval time.Duration     `json:"interval" configKey:"interval" configUsage:"Duration from the last import." validate:"required,minDuration=60s,maxDuration=24h"`
}

// With copies values from the ConfigPatch, if any.
func (c Config) With(v ConfigPatch) Config {
	if v.Import != nil {
		if v.Import.Trigger != nil {
			c.Import.Trigger = *v.Import.Trigger
		}
	}
	return c
}

func NewConfig() Config {
	return Config{
		Import: ImportConfig{
			MinInterval: 1 * time.Minute,
			Trigger:     DefaultFileImportTrigger(),
		},
	}
}

// DefaultFileImportTrigger determines when a sliced file will be imported to the table.
func DefaultFileImportTrigger() FileImportTrigger {
	return FileImportTrigger{
		Count:    50000,
		Size:     5 * datasize.MB,
		Interval: 5 * time.Minute,
	}
}
