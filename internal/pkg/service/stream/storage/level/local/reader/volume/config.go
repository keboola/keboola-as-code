package volume

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	defaultVolumeIDWaitTimeout = 30 * time.Second
)

type config struct {
	// waitForVolumeIDTimeout defines how long to wait for the existence of a file with the ID,
	// see Open function and Volume.waitForVolumeID method.
	waitForVolumeIDTimeout time.Duration
	// fileOpener provides file opening, a custom implementation can be useful for tests.
	fileOpener FileOpener
}

type Option func(config *config)

func newConfig(opts []Option) config {
	cfg := config{
		waitForVolumeIDTimeout: defaultVolumeIDWaitTimeout,
		fileOpener:             DefaultFileOpener,
	}

	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

func WithWaitForVolumeIDTimeout(v time.Duration) Option {
	return func(c *config) {
		if v <= 0 {
			panic(errors.New(`value must be greater than zero`))
		}
		c.waitForVolumeIDTimeout = v
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
