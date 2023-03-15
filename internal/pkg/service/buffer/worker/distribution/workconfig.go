package distribution

import (
	"time"
)

const (
	DefaultResetInterval = 5 * time.Minute
)

type WorkOption func(c *workConfig)

type workConfig struct {
	restartInterval time.Duration
}

func newWorkConfig(opts []WorkOption) workConfig {
	c := workConfig{restartInterval: DefaultResetInterval}
	for _, o := range opts {
		o(&c)
	}
	return c
}

// WithResetInterval defines periodical reset interval of the DistributedWork.
func WithResetInterval(v time.Duration) WorkOption {
	return func(c *workConfig) {
		c.restartInterval = v
	}
}
