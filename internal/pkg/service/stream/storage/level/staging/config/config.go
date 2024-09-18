package config

import (
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
)

// Config configures for the staging storage.
type Config struct {
	Operator OperatorConfig `configKey:"operator"`
	Upload   UploadConfig   `configKey:"upload"`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ConfigPatch struct {
	Upload *UploadConfigPatch `json:"upload,omitempty"`
}

func NewConfig() Config {
	return Config{
		Operator: OperatorConfig{
			SliceRotationCheckInterval: duration.From(1 * time.Second),
			SliceRotationTimeout:       duration.From(5 * time.Minute),
			SliceCloseTimeout:          duration.From(1 * time.Minute),
			SliceUploadCheckInterval:   duration.From(2 * time.Second),
			SliceUploadTimeout:         duration.From(15 * time.Minute),
		},
		Upload: UploadConfig{
			MinInterval: duration.From(10 * time.Second),
			Trigger: UploadTrigger{
				Count:    10000,
				Size:     5 * datasize.MB,
				Interval: duration.From(30 * time.Second),
			},
		},
	}
}

type OperatorConfig struct {
	SliceRotationCheckInterval duration.Duration `json:"sliceRotationCheckInterval" configKey:"sliceRotationCheckInterval" configUsage:"Upload triggers check interval." validate:"required,minDuration=100ms,maxDuration=30s"`
	SliceRotationTimeout       duration.Duration `json:"sliceRotationTimeout" configKey:"sliceRotationTimeout" configUsage:"Timeout of the slice rotation operation." validate:"required,minDuration=30s,maxDuration=15m"`
	SliceCloseTimeout          duration.Duration `json:"sliceCloseTimeout" configKey:"sliceCloseTimeout" configUsage:"Timeout of the slice close operation." validate:"required,minDuration=10s,maxDuration=10m"`
	SliceUploadCheckInterval   duration.Duration `json:"sliceUploadCheckInterval" configKey:"sliceUploadCheckInterval" configUsage:"Interval of checking slices in the 'uploading' state to perform upload." validate:"required,minDuration=500ms,maxDuration=30s"`
	SliceUploadTimeout         duration.Duration `json:"sliceUploadTimeout" configKey:"sliceUploadTimeout" configUsage:"Timeout of the slice upload operation." validate:"required,minDuration=30s,maxDuration=60m"`
}

// UploadConfig configures the slice upload.
type UploadConfig struct {
	MinInterval duration.Duration `json:"minInterval" configKey:"minInterval" configUsage:"Min duration from the last upload to trigger the next, takes precedence over other settings." validate:"required,minDuration=1s,maxDuration=30m"`
	Trigger     UploadTrigger     `json:"trigger" configKey:"trigger"`
}

// UploadConfigPatch is same as the UploadConfig, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type UploadConfigPatch struct {
	Trigger *UploadTriggerPatch `json:"trigger,omitempty"`
}

// UploadTrigger configures slice upload conditions, at least one must be met.
type UploadTrigger struct {
	Count    uint64            `json:"count" configKey:"count" configUsage:"Records count to trigger slice upload." modAllowed:"true" validate:"required,min=1,max=10000000"`
	Size     datasize.ByteSize `json:"size" configKey:"size" configUsage:"Records size to trigger slice upload." modAllowed:"true" validate:"required,minBytes=100B,maxBytes=50MB"`
	Interval duration.Duration `json:"interval" configKey:"interval" configUsage:"Duration from the last slice upload to trigger the next upload." modAllowed:"true" validate:"required,minDuration=1s,maxDuration=30m"`
}

// UploadTriggerPatch is same as the UploadTrigger, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type UploadTriggerPatch struct {
	Count    *uint64            `json:"count,omitempty" configKey:"count"`
	Size     *datasize.ByteSize `json:"size,omitempty" configKey:"size"`
	Interval *duration.Duration `json:"interval,omitempty" configKey:"interval"`
}
