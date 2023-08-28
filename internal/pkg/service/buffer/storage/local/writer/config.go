package writer

type config struct {
}

type Option func(config *config)

func newConfig(opts []Option) config {
	cfg := config{}

	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}
