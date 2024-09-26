package task

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	DefaultSessionTTL = 15 // seconds, see WithTTL
	EtcdPrefix = "task"
)

type NodeOption func(c *nodeConfig)

type nodeConfig struct {
	ttlSeconds int
}

func defaultNodeConfig() nodeConfig {
	return nodeConfig{ttlSeconds: DefaultSessionTTL}
}

// WithTTL defines time after the session is canceled if the client is unavailable.
// Client sends periodic keep-alive requests.
func WithTTL(v int) NodeOption {
	return func(c *nodeConfig) {
		c.ttlSeconds = v
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
