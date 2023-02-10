package apinode

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	DefaultSyncInterval = 1000 * time.Millisecond
	DefaultSessionTTL   = 15 // seconds, see WithTTL
)

type Option func(c *config)

type config struct {
	syncInterval time.Duration
	ttlSeconds   int
}

func defaultConfig() config {
	return config{syncInterval: DefaultSyncInterval, ttlSeconds: DefaultSessionTTL}
}

// WithSyncInterval defines how often will the revision be synchronized to etcd.
// Synchronization occurs only if the value has changed.
func WithSyncInterval(v time.Duration) Option {
	if v <= 0 {
		panic(errors.New("sync interval value ust be positive"))
	}
	return func(c *config) {
		c.syncInterval = v
	}
}

// WithTTL defines time after the session is canceled if the client is unavailable.
// Client sends periodic keep-alive requests.
func WithTTL(v int) Option {
	if v <= 0 {
		panic(errors.New("ttl seconds value must be positive"))
	}
	return func(c *config) {
		c.ttlSeconds = v
	}
}
