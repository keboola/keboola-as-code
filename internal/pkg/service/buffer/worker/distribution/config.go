package distribution

import (
	"time"
)

const (
	DefaultSessionTTL           = 15               // seconds, see WithTTL
	DefaultStartupTimeout       = 60 * time.Second // timeout for registration, PUT operation
	DefaultShutdownTimeout      = 5 * time.Second  // timeout for un-registration, DELETE operation
	DefaultSelfDiscoveryTimeout = 30 * time.Second // timeout, how long the Node should wait to discover itself back by the etcd watcher.
	DefaultEventsGroupInterval  = 5 * time.Second  // all changes in the interval are grouped together, so that updates do not occur too often
)

type Option func(c *config)

type config struct {
	startupTimeout       time.Duration
	shutdownTimeout      time.Duration
	selfDiscoveryTimeout time.Duration
	eventsGroupInterval  time.Duration
	ttlSeconds           int
}

func defaultConfig() config {
	return config{
		startupTimeout:       DefaultStartupTimeout,
		shutdownTimeout:      DefaultShutdownTimeout,
		selfDiscoveryTimeout: DefaultSelfDiscoveryTimeout,
		eventsGroupInterval:  DefaultEventsGroupInterval,
		ttlSeconds:           DefaultSessionTTL,
	}
}

// WithStartupTimeout defines node registration timeout on the node startup.
func WithStartupTimeout(v time.Duration) Option {
	return func(c *config) {
		c.startupTimeout = v
	}
}

// WithShutdownTimeout defines node un-registration timeout on the node shutdown.
func WithShutdownTimeout(v time.Duration) Option {
	return func(c *config) {
		c.shutdownTimeout = v
	}
}

// WithSelfDiscoveryTimeout defines how long the Node should wait to discover itself back by the etcd watcher.
func WithSelfDiscoveryTimeout(v time.Duration) Option {
	return func(c *config) {
		c.selfDiscoveryTimeout = v
	}
}

// WithEventsGroupInterval defines events grouping interval.
// All changes in the interval are grouped together, so that updates do not occur too often.
func WithEventsGroupInterval(v time.Duration) Option {
	return func(c *config) {
		c.eventsGroupInterval = v
	}
}

// WithTTL defines time after the session is canceled if the client is unavailable.
// Client sends periodic keep-alive requests.
func WithTTL(v int) Option {
	return func(c *config) {
		c.ttlSeconds = v
	}
}
