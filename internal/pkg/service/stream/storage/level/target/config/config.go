package config

import (
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
)

// Config configures the target storage.
type Config struct {
	Operator OperatorConfig `configKey:"operator"`
	Import   ImportConfig   `configKey:"import"`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ConfigPatch struct {
	Import *ImportConfigPatch `json:"import,omitempty"`
}

func NewConfig() Config {
	return Config{
		Operator: OperatorConfig{
			FileRotationCheckInterval: duration.From(1 * time.Second),
			FileRotationTimeout:       duration.From(5 * time.Minute),
			FileCloseTimeout:          duration.From(1 * time.Minute),
			FileImportCheckInterval:   duration.From(1 * time.Second),
			FileImportTimeout:         duration.From(15 * time.Minute),
		},
		Import: ImportConfig{
			MaxSlices:   200,
			MinInterval: duration.From(60 * time.Second),
			Trigger: ImportTrigger{
				Count:       50000,
				Size:        50 * datasize.MB,
				Interval:    duration.From(1 * time.Minute),
				SlicesCount: 100,
				Expiration:  duration.From(30 * time.Minute),
			},
		},
	}
}

type OperatorConfig struct {
	FileRotationCheckInterval duration.Duration `json:"fileRotationCheckInterval" configKey:"fileRotationCheckInterval" configUsage:"Import triggers check interval." validate:"required,minDuration=100ms,maxDuration=30s"`
	FileRotationTimeout       duration.Duration `json:"fileRotationTimeout" configKey:"fileRotationTimeout" configUsage:"Timeout of the file rotation operation." validate:"required,minDuration=30s,maxDuration=15m"`
	FileCloseTimeout          duration.Duration `json:"fileCloseTimeout" configKey:"fileCloseTimeout" configUsage:"Timeout of the file close operation." validate:"required,minDuration=10s,maxDuration=10m"`
	FileImportCheckInterval   duration.Duration `json:"fileImportCheckInterval" configKey:"fileImportCheckInterval" configUsage:"Interval of checking files in the importing state." validate:"required,minDuration=500ms,maxDuration=30s"`
	FileImportTimeout         duration.Duration `json:"fileImportTimeout" configKey:"fileImportTimeout" configUsage:"Timeout of the file import operation." validate:"required,minDuration=30s,maxDuration=60m"`
}

// ImportConfig configures the file import.
type ImportConfig struct {
	MaxSlices   uint64            `json:"maxSlices" configKey:"maxSlices" configUsage:"Max number of slices in a file before an import is triggered, takes precedence over other settings." validate:"required,min=100,max=200"`
	MinInterval duration.Duration `json:"minInterval" configKey:"minInterval" configUsage:"Min duration from the last import to trigger the next, takes precedence over other settings." validate:"required,minDuration=30s,maxDuration=24h"`
	Trigger     ImportTrigger     `json:"trigger" configKey:"trigger"`
}

// ImportConfigPatch is same as the ImportConfig, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ImportConfigPatch struct {
	Trigger *ImportTriggerPatch `json:"trigger,omitempty"`
}

// ImportTrigger configures file import conditions, at least one must be met.
type ImportTrigger struct {
	Count       uint64            `json:"count" configKey:"count" configUsage:"Records count to trigger file import." modAllowed:"true" validate:"required,min=1,max=10000000"`
	Size        datasize.ByteSize `json:"size" configKey:"size" configUsage:"Records size to trigger file import." modAllowed:"true" validate:"required,minBytes=100B,maxBytes=500MB"`
	Interval    duration.Duration `json:"interval" configKey:"interval" configUsage:"Duration from the last import to trigger the next import." modAllowed:"true" validate:"required,minDuration=30s,maxDuration=24h"`
	SlicesCount uint64            `json:"slicesCount" configKey:"slicesCount" configUsage:"Number of slices in the file to trigger file import." validate:"required,min=1,max=200"`
	Expiration  duration.Duration `json:"expiration" configKey:"expiration" configUsage:"Min remaining expiration to trigger file import." validate:"required,minDuration=5m,maxDuration=45m"`
}

// ImportTriggerPatch is same as the ImportTrigger, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ImportTriggerPatch struct {
	Count       *uint64            `json:"count,omitempty"`
	Size        *datasize.ByteSize `json:"size,omitempty"`
	Interval    *duration.Duration `json:"interval,omitempty"`
	SlicesCount *uint64            `json:"slicesCount,omitempty"`
	Expiration  *duration.Duration `json:"expiration,omitempty"`
}
