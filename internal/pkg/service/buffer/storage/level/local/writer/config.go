package writer

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local/writer/allocate"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type config struct {
	// allocator allocates a free disk space for a file.
	allocator allocate.Allocator
	// writerFactory creates a high-level writer for the storage.FileType, for example storage.FileTypeCSV.
	writerFactory Factory
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
		allocator:      allocate.DefaultAllocator{},
		writerFactory:  DefaultFactory,
		fileOpener:     DefaultFileOpener,
		watchDrainFile: true,
	}

	for _, o := range opts {
		o(&cfg)
	}

	return cfg
}

func WithAllocator(v allocate.Allocator) Option {
	return func(c *config) {
		if v == nil {
			panic(errors.New(`value must not be nil`))
		}
		c.allocator = v
	}
}

func WithWriterFactory(v Factory) Option {
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
