package distribution

import (
	"time"
)

const (
	DefaultResetInterval = 5 * time.Minute
)

type ExecutorOption func(c *executorConfig)

type executorConfig struct {
	resetInterval time.Duration
}

func defaultExecutorConfig() executorConfig {
	return executorConfig{resetInterval: DefaultResetInterval}
}

// WithResetInterval defines periodical reset interval of the ExecutorWork.
func WithResetInterval(v time.Duration) ExecutorOption {
	return func(c *executorConfig) {
		c.resetInterval = v
	}
}
