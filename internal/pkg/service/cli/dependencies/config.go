package dependencies

type config struct {
	defaultStorageAPIHost string
	withoutMasterToken    bool
}

type Option func(*config)

func newConfig(ops []Option) config {
	c := config{}
	for _, o := range ops {
		o(&c)
	}
	return c
}

// WithDefaultStorageAPIHost enable the use of the default Storage API host if none is specified.
// By default, a host must always be specified.
// This is useful for commands that only need a list of components and do not depend on a specific stack.
func WithDefaultStorageAPIHost() Option {
	return func(c *config) {
		c.defaultStorageAPIHost = "connection.keboola.com"
	}
}

// WithoutMasterToken disables the requirement to provide a master token any valid token will be accepted.
func WithoutMasterToken() Option {
	return func(c *config) {
		c.withoutMasterToken = true
	}
}
