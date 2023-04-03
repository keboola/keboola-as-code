package task

import (
	"context"

	taskKey "github.com/keboola/keboola-as-code/internal/pkg/service/common/task/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	DefaultSessionTTL     = 15 // seconds, see WithTTL
	DefaultTaskEtcdPrefix = "task"
)

type NodeOption func(c *nodeConfig)

type nodeConfig struct {
	spanNamePrefix string
	taskEtcdPrefix string
	ttlSeconds     int
}

func defaultNodeConfig() nodeConfig {
	return nodeConfig{taskEtcdPrefix: DefaultTaskEtcdPrefix, ttlSeconds: DefaultSessionTTL}
}

// WithSpanNamePrefix defines prefix for tracing spans.
func WithSpanNamePrefix(p string) NodeOption {
	return func(c *nodeConfig) {
		c.spanNamePrefix = p
	}
}

// WithTaskEtcdPrefix defines prefix for tasks records in etcd.
func WithTaskEtcdPrefix(p string) NodeOption {
	return func(c *nodeConfig) {
		c.taskEtcdPrefix = p
	}
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
	Key       taskKey.Key
	Lock      string
	Context   ContextFactory
	Operation Task
}

type ContextFactory func() (context.Context, context.CancelFunc)

func (c Config) Validate() error {
	errs := errors.NewMultiError()
	if c.Type == "" {
		errs.Append(errors.New("task type must be configured"))
	}
	if c.Key == (taskKey.Key{}) {
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
