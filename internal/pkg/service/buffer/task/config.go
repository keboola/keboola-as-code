package task

const (
	DefaultSessionTTL = 15 // seconds, see WithTTL
)

type Option func(c *config)

type config struct {
	ttlSeconds int
}

func defaultConfig() config {
	return config{ttlSeconds: DefaultSessionTTL}
}

// WithTTL defines time after the session is canceled if the client is unavailable.
// Client sends periodic keep-alive requests.
func WithTTL(v int) Option {
	return func(c *config) {
		c.ttlSeconds = v
	}
}
