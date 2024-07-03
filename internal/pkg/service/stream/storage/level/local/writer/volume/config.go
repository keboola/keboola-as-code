package volume

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/sourcenode/format/factory"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/writernode/diskalloc"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type config struct {
	writerConfig writer.Config
	// allocator allocates a free disk space for a file.
	allocator diskalloc.Allocator
	// formatWriterFactory creates a high-level writer for the storage.FileType, for example storage.FileTypeCSV.
	formatWriterFactory writer.FormatWriterFactory
	// syncerFactory provides disksync.Syncer a custom implementation can be useful for tests.
	syncerFactory disksync.SyncerFactory
	// fileOpener provides file opening, a custom implementation can be useful for tests.
	fileOpener FileOpener
	// watchDrainFile activates watching for drainFile changes (creation/deletion),
	// otherwise the file is checked only on the volume opening.
	// Default Linux OS limit is 128 inotify watchers = 128 volumes.
	// The value is sufficient for production but insufficient parallel for tests.
	watchDrainFile bool
}

type Option func(config *config)

func newConfig(wrCfg writer.Config, opts []Option) config {
	cfg := config{
		writerConfig:        wrCfg,
		allocator:           diskalloc.DefaultAllocator{},
		syncerFactory:       disksync.NewSyncer,
		formatWriterFactory: factory.Default,
		fileOpener:          DefaultFileOpener,
		watchDrainFile:      true,
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

func WithSyncerFactory(v disksync.SyncerFactory) Option {
	return func(c *config) {
		c.syncerFactory = v
	}
}

func WithFormatWriterFactory(v writer.FormatWriterFactory) Option {
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
