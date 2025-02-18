package diskwriter

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/diskalloc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
)

type Config struct {
	Network network.Config `configKey:"network"`
	// WatchDrainFile activates watching for drainFile changes (creation/deletion),
	// otherwise the file is checked only on the volume opening.
	// Default Linux OS limit is 128 inotify watchers = 128 volumes.
	// The value is sufficient for production but insufficient parallel for tests.
	WatchDrainFile bool
	// Allocation configures allocation of the disk space for file slices.
	Allocation diskalloc.Config `configKey:"allocation"`
	// OverrideFileOpener overrides file opening.
	// A custom implementation can be useful for tests.
	OverrideFileOpener FileOpener
	// UseBackupWriter determines whether to use temporary file during write operations.
	// When enabled, data is written to a temporary file first and moved to the final location on close.
	UseBackupWriter bool `configKey:"useBackupWriter"`
}

func NewConfig() Config {
	return Config{
		Network:        network.NewConfig(),
		WatchDrainFile: true,
		Allocation:     diskalloc.NewConfig(),
	}
}
