package service

import (
	"net/http"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
)

// DefaultCheckConditionsInterval defines how often it will be checked upload and import conditions.
const DefaultCheckConditionsInterval = 30 * time.Second

type config struct {
	checkConditions         bool
	cleanup                 bool
	closeFiles              bool
	closeSlices             bool
	importFiles             bool
	retryFailedFiles        bool
	retryFailedSlices       bool
	uploadSlices            bool
	uploadTransport         http.RoundTripper
	checkConditionsInterval time.Duration
	uploadConditions        model.Conditions
}

type Option func(c *config)

func newConfig(ops []Option) config {
	c := config{
		checkConditions:         true,
		cleanup:                 true,
		closeFiles:              true,
		closeSlices:             true,
		importFiles:             true,
		retryFailedFiles:        true,
		retryFailedSlices:       true,
		uploadSlices:            true,
		checkConditionsInterval: DefaultCheckConditionsInterval,
		uploadConditions:        model.DefaultUploadConditions(),
	}
	for _, o := range ops {
		o(&c)
	}
	return c
}

func WithCheckConditionsInterval(v time.Duration) Option {
	return func(c *config) {
		c.checkConditionsInterval = v
	}
}

func WithUploadConditions(v model.Conditions) Option {
	return func(c *config) {
		c.uploadConditions = v
	}
}

// WithCheckConditions enables/disables the conditions checker.
func WithCheckConditions(v bool) Option {
	return func(c *config) {
		c.checkConditions = v
	}
}

// WithCleanup enables/disables etcd cleanup task.
func WithCleanup(v bool) Option {
	return func(c *config) {
		c.cleanup = v
	}
}

// WithCloseSlices enables/disables the "close slices" task.
func WithCloseSlices(v bool) Option {
	return func(c *config) {
		c.closeSlices = v
	}
}

// WithUploadSlices enables/disables the "upload slices" task.
func WithUploadSlices(v bool) Option {
	return func(c *config) {
		c.uploadSlices = v
	}
}

// WithRetryFailedSlices enables/disables the "retry failed uploads" task.
func WithRetryFailedSlices(v bool) Option {
	return func(c *config) {
		c.retryFailedSlices = v
	}
}

// WithUploadTransport overwrites default HTTP transport.
func WithUploadTransport(v http.RoundTripper) Option {
	return func(c *config) {
		c.uploadTransport = v
	}
}

// WithCloseFiles enables/disables the "close files" task.
func WithCloseFiles(v bool) Option {
	return func(c *config) {
		c.closeFiles = v
	}
}

// WithImportFiles enables/disables the "upload file" task.
func WithImportFiles(v bool) Option {
	return func(c *config) {
		c.importFiles = v
	}
}

// WithRetryFailedFiles enables/disables the "retry failed imports" task.
func WithRetryFailedFiles(v bool) Option {
	return func(c *config) {
		c.retryFailedFiles = v
	}
}
