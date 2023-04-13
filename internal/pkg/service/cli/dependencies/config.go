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

func WithDefaultStorageAPIHost() Option {
	return func(c *config) {
		c.defaultStorageAPIHost = "connection.keboola.com"
	}
}

func WithoutMasterToken() Option {
	return func(c *config) {
		c.withoutMasterToken = true
	}
}
