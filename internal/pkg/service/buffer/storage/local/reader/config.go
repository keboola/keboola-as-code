package reader

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"time"
)

const (
	defaultVolumeIDWaitTimeout = 30 * time.Second
)

type config struct {
	// waitForVolumeIDTimeout defines how long to wait for the existence of a file with the VolumeID,
	// see OpenVolume function and Volume.waitForVolumeID method.
	waitForVolumeIDTimeout time.Duration
}

type Option func(config *config)

func WithWaitForVolumeIDTimeout(v time.Duration) Option {
	return func(c *config) {
		if v <= 0 {
			panic(errors.New(`value must be greater than zero`))
		}
		c.waitForVolumeIDTimeout = v
	}
}

func newConfig(opts []Option) config {
	cfg := config{
		waitForVolumeIDTimeout: defaultVolumeIDWaitTimeout,
	}

	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}
