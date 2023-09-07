package writer

import "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/allocate"

type config struct {
	allocator     allocate.Allocator
	writerFactory Factory
}

type Option func(config *config)

func newConfig(opts []Option) config {
	cfg := config{
		allocator:     allocate.DefaultAllocator{},
		writerFactory: DefaultFactory,
	}

	for _, o := range opts {
		o(&cfg)
	}

	return cfg
}

func WithAllocator(v allocate.Allocator) Option {
	return func(c *config) {
		c.allocator = v
	}
}

func WithWriterFactory(v Factory) Option {
	return func(c *config) {
		c.writerFactory = v
	}
}
