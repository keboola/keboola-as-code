package config

import (
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
)

// Config configures for the staging storage.
type Config struct {
	MaxSlicesPerFile        int          `configKey:"maxSlicesPerFile" configUsage:"Maximum number of slices in a file, a new file is created after reaching it." validate:"required,min=1,max=50000"`
	ParallelFileCreateLimit int          `configKey:"parallelFileCreateLimit" configUsage:"Maximum number of the Storage API file resources created in parallel within one operation." validate:"required,min=1,max=500"`
	Upload                  UploadConfig `configKey:"upload"`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ConfigPatch struct {
	MaxSlicesPerFile *int               `json:"maxSlicesPerFile,omitempty"`
	Upload           *UploadConfigPatch `json:"upload,omitempty"`
}

func NewConfig() Config {
	return Config{
		MaxSlicesPerFile:        100,
		ParallelFileCreateLimit: 50,
		Upload: UploadConfig{
			MinInterval: duration.From(5 * time.Second),
			Trigger: UploadTrigger{
				Count:    10000,
				Size:     1 * datasize.MB,
				Interval: duration.From(1 * time.Minute),
			},
		},
	}
}

// UploadConfig configures the slice upload.
type UploadConfig struct {
	MinInterval duration.Duration `json:"minInterval" configKey:"minInterval" configUsage:"Minimal interval between uploads." validate:"required,minDuration=1s,maxDuration=5m"`
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
