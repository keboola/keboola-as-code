package revision

import (
	"time"
)

const (
	DefaultSyncInterval = 100 * time.Millisecond
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
	return func(c *config) {
		c.syncInterval = v
	}
}

// WithTTL defines time after the session is canceled if the client is unavailable.
// Client sends periodic keep-alive requests.
func WithTTL(v int) Option {
	return func(c *config) {
		c.ttlSeconds = v
	}
}
