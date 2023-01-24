package service

import (
	"net/http"
)

type config struct {
	checkConditions   bool
	closeSlices       bool
	uploadSlices      bool
	retryFailedSlices bool
	uploadTransport   http.RoundTripper
}

type Option func(c *config)

func newConfig(ops []Option) config {
	c := config{
		checkConditions:   true,
		closeSlices:       true,
		uploadSlices:      true,
		retryFailedSlices: true,
	}
	for _, o := range ops {
		o(&c)
	}
	return c
}

// WithCheckConditions enables/disables the coditions checker.
func WithCheckConditions(v bool) Option {
	return func(c *config) {
		c.checkConditions = v
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
