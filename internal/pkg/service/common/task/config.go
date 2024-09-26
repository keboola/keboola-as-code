package task

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	EtcdPrefix = "task"
)

type NodeConfig struct {
	TTLSeconds      int           `configKey:"ttlSeconds" configUsage:"Defines time after the session is canceled if the client is unavailable." validate:"required"`
	CleanupEnabled  bool          `configKey:"cleanupEnabled" configUsage:"Enable periodical tasks cleanup functionality."`
	CleanupInterval time.Duration `configKey:"cleanupInterval" configUsage:"How often will old tasks be deleted." validate:"required"`
}

func NewNodeConfig() NodeConfig {
	return NodeConfig{
		TTLSeconds:      15,
		CleanupEnabled:  true,
		CleanupInterval: 1 * time.Hour,
	}
}

type Config struct {
	Type      string
	Key       Key
	Lock      string
	Context   ContextFactory
	Operation Fn
}

type ContextFactory func() (context.Context, context.CancelFunc)

func (c Config) Validate() error {
	errs := errors.NewMultiError()
	if c.Type == "" {
		errs.Append(errors.New("task type must be configured"))
	}
	if c.Key == (Key{}) {
		errs.Append(errors.New("task key must be configured"))
	}
	if c.Context == nil {
		errs.Append(errors.New("task context factory must be configured"))
	}
	if c.Operation == nil {
		errs.Append(errors.New("task operation must be configured"))
	}
	return errs.ErrorOrNil()
}
