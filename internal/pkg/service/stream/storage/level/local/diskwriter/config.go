package diskwriter

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/diskalloc"
)

type Config struct {
	// WatchDrainFile activates watching for drainFile changes (creation/deletion),
	// otherwise the file is checked only on the volume opening.
	// Default Linux OS limit is 128 inotify watchers = 128 volumes.
	// The value is sufficient for production but insufficient parallel for tests.
	WatchDrainFile bool
	// Allocator allocates a free disk space for a file.
	// A custom implementation can be useful for tests.
	Allocator diskalloc.Allocator
	// FileOpener provides file opening.
	// A custom implementation can be useful for tests.
	FileOpener FileOpener
}

type ConfigPatch struct {
}

func NewConfig() Config {
	return Config{
		WatchDrainFile: true,
		Allocator:      diskalloc.DefaultAllocator{},
		FileOpener:     DefaultFileOpener,
	}
}
