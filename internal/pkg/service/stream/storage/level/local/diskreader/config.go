package diskreader

import (
	"time"
)

type Config struct {
	// WaitForVolumeIDTimeout defines how long to wait for the existence of a file with the ID,
	// see Open function and Volume.waitForVolumeID method.
	WaitForVolumeIDTimeout time.Duration
	// OverwriteFileOpener overwrites file opening.
	// A custom implementation can be useful for tests.
	OverwriteFileOpener FileOpener
}

func NewConfig() Config {
	return Config{
		WaitForVolumeIDTimeout: 30 * time.Second,
	}
}
