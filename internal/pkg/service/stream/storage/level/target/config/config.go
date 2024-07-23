package config

import (
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
)

// Config configures the target storage.
type Config struct {
	Import ImportConfig `configKey:"import"`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ConfigPatch struct {
	Import *ImportConfigPatch `json:"import,omitempty"`
}

func NewConfig() Config {
	return Config{
		Import: ImportConfig{
			MinInterval: duration.From(1 * time.Minute),
			Trigger: ImportTrigger{
				Count:    50000,
				Size:     5 * datasize.MB,
				Interval: duration.From(5 * time.Minute),
			},
		},
	}
}

// ImportConfig configures the file import.
type ImportConfig struct {
	MinInterval duration.Duration `json:"minInterval" configKey:"minInterval" configUsage:"Minimal interval between imports." validate:"required,minDuration=30s,maxDuration=30m"`
	Trigger     ImportTrigger     `json:"trigger" configKey:"trigger"`
}

// ImportConfigPatch is same as the ImportConfig, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ImportConfigPatch struct {
	Trigger *ImportTriggerPatch `json:"trigger,omitempty"`
}

// ImportTrigger configures file import conditions, at least one must be met.
type ImportTrigger struct {
	Count    uint64            `json:"count" configKey:"count" configUsage:"Records count to trigger file import." modAllowed:"true" validate:"required,min=1,max=10000000"`
	Size     datasize.ByteSize `json:"size" configKey:"size" configUsage:"Records size to trigger file import." modAllowed:"true" validate:"required,minBytes=100B,maxBytes=500MB"`
	Interval duration.Duration `json:"interval" configKey:"interval" configUsage:"Duration from the last import to trigger the next import." modAllowed:"true" validate:"required,minDuration=60s,maxDuration=24h"`
}

// ImportTriggerPatch is same as the ImportTrigger, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ImportTriggerPatch struct {
	Count    *uint64            `json:"count,omitempty" configKey:"count"`
	Size     *datasize.ByteSize `json:"size,omitempty" configKey:"size"`
	Interval *duration.Duration `json:"interval,omitempty" configKey:"interval"`
}
