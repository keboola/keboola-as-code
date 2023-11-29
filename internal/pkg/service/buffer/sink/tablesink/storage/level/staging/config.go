package staging

import (
	"github.com/c2h5oh/datasize"
	"time"
)

// Config contains default configuration for the staging storage.
type Config struct {
	Upload UploadConfig `configKey:"upload"`
}

type UploadConfig struct {
	MinInterval time.Duration      `configKey:"minInterval" configUsage:"Minimal duration between uploads." validate:"required,minDuration=1s,maxDuration=5m"`
	Trigger     SliceUploadTrigger `configKey:"trigger"`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It is part of the definition.TableSink structure to allow modification of the default configuration.
type ConfigPatch struct {
	Upload *UploadConfigPatch `json:"upload,omitempty"`
}

type UploadConfigPatch struct {
	Trigger *SliceUploadTrigger `json:"trigger,omitempty"`
}

// With copies values from the ConfigPatch, if any.
func (c Config) With(v ConfigPatch) Config {
	if v.Upload != nil {
		if v.Upload.Trigger != nil {
			c.Upload.Trigger = *v.Upload.Trigger
		}
	}
	return c
}

func NewConfig() Config {
	return Config{
		Upload: UploadConfig{
			MinInterval: 5 * time.Second,
			Trigger:     DefaultSliceUploadTrigger(),
		},
	}
}

// SliceUploadTrigger struct configures conditions for slice upload to the staging storage.
type SliceUploadTrigger struct {
	Count    uint64            `json:"count" configKey:"count" configUsage:"Records count." validate:"required,min=1,max=10000000"`
	Size     datasize.ByteSize `json:"size" configKey:"size" configUsage:"Records size." validate:"required,minBytes=100B,maxBytes=50MB"`
	Interval time.Duration     `json:"interval" configKey:"interval" configUsage:"Duration from the last upload." validate:"required,minDuration=1s,maxDuration=30m"`
}

// DefaultSliceUploadTrigger determines when a file slice will be imported to the storage.
func DefaultSliceUploadTrigger() SliceUploadTrigger {
	return SliceUploadTrigger{
		Count:    10000,
		Size:     1 * datasize.MB,
		Interval: 1 * time.Minute,
	}
}
