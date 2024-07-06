package volume

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/diskalloc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/format/factory"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type config struct {
	local.Config
	// allocator allocates a free disk space for a file.
	allocator diskalloc.Allocator
	// formatWriterFactory creates a high-level writer for the storage.FileType, for example storage.FileTypeCSV.
	formatWriterFactory encoding.EncoderFactory
	// syncerFactory provides writesync.Syncer a custom implementation can be useful for tests.
	syncerFactory writesync.SyncerFactory
	// fileOpener provides file opening, a custom implementation can be useful for tests.
	fileOpener FileOpener
	// watchDrainFile activates watching for drainFile changes (creation/deletion),
	// otherwise the file is checked only on the volume opening.
	// Default Linux OS limit is 128 inotify watchers = 128 volumes.
	// The value is sufficient for production but insufficient parallel for tests.
	watchDrainFile bool
}

type Option func(config *config)

func newConfig(cfg local.Config, opts []Option) config {
	c := config{
		Config:              cfg,
		allocator:           diskalloc.DefaultAllocator{},
		syncerFactory:       writesync.NewSyncer,
		formatWriterFactory: factory.Default,
		fileOpener:          DefaultFileOpener,
		watchDrainFile:      true,
	}

	for _, o := range opts {
		o(&c)
	}

	return c
}

func WithAllocator(v diskalloc.Allocator) Option {
	return func(c *config) {
		if v == nil {
			panic(errors.New(`value must not be nil`))
		}
		c.allocator = v
	}
}

func WithSyncerFactory(v writesync.SyncerFactory) Option {
	return func(c *config) {
		c.syncerFactory = v
	}
}

func WithFormatWriterFactory(v encoding.EncoderFactory) Option {
	return func(c *config) {
		if v == nil {
			panic(errors.New(`value must not be nil`))
		}

		c.formatWriterFactory = v
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
