package volume

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/diskalloc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/factory"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type config struct {
	// allocator allocates a free disk space for a file.
	allocator diskalloc.Allocator
	// writerFactory creates a high-level writer for the storage.FileType, for example storage.FileTypeCSV.
	writerFactory factory.Factory
	// fileOpener provides file opening, a custom implementation can be useful for tests.
	fileOpener FileOpener
	// watchDrainFile activates watching for drainFile changes (creation/deletion),
	// otherwise the file is checked only on the volume opening.
	// Default Linux OS limit is 128 inotify watchers = 128 volumes.
	// The value is sufficient for production but insufficient parallel for tests.
	watchDrainFile bool
}

type Option func(config *config)

func newConfig(opts []Option) config {
	cfg := config{
		allocator:      diskalloc.DefaultAllocator{},
		writerFactory:  factory.Default,
		fileOpener:     DefaultFileOpener,
		watchDrainFile: true,
	}

	for _, o := range opts {
		o(&cfg)
	}

	return cfg
}

func WithAllocator(v diskalloc.Allocator) Option {
	return func(c *config) {
		if v == nil {
			panic(errors.New(`value must not be nil`))
		}
		c.allocator = v
	}
}

func WithWriterFactory(v factory.Factory) Option {
	return func(c *config) {
		if v == nil {
			panic(errors.New(`value must not be nil`))
		}
		c.writerFactory = v
	}
}

func WithFileOpener(v FileOpener) Option {
	return func(c *config) {
		if v == nil {
			panic(errors.New(`value must not be nil`))
		}
		c.fileOpener = v
	}
}

func WithWatchDrainFile(v bool) Option {
	return func(c *config) {
		c.watchDrainFile = v
	}
}
