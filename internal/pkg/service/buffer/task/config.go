package task

const (
	DefaultSessionTTL = 15 // seconds, see WithTTL
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

type config struct {
	lock string
}

type Option func(*config)

func WithLock(v string) Option {
	return func(c *config) {
		c.lock = v
	}
}

func defaultTaskConfig() config {
	return config{}
}
