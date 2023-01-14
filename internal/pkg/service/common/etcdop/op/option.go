package op

import (
	"time"
)

const (
	DefaultRetryInitialInterval = 10 * time.Millisecond
	DefaultRetryMaxInterval     = 2 * time.Second
	DefaultRetryMaxElapsedTime  = 15 * time.Second
)

type Option func(*config)

type config struct {
	retryInitialInterval time.Duration
	retryMaxInterval     time.Duration
	retryMaxElapsedTime  time.Duration
}

func newConfig(opts []Option) config {
	c := config{
		retryInitialInterval: DefaultRetryInitialInterval,
		retryMaxInterval:     DefaultRetryMaxInterval,
		retryMaxElapsedTime:  DefaultRetryMaxElapsedTime,
	}

	for _, o := range opts {
		o(&c)
	}

	return c
}

func WithRetryInitialInterval(v time.Duration) Option {
	return func(c *config) {
		c.retryInitialInterval = v
	}
}

func WithRetryMaxInterval(v time.Duration) Option {
	return func(c *config) {
		c.retryMaxInterval = v
	}
}

func WithRetryMaxElapsedTime(v time.Duration) Option {
	return func(c *config) {
		c.retryMaxElapsedTime = v
	}
}
