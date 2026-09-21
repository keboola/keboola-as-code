package encoding

import (
	"time"

	"github.com/cenkalti/backoff/v5"
)

func newChunkBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0.1
	b.Multiplier = 2
	b.InitialInterval = 100 * time.Millisecond
	b.MaxInterval = 15 * time.Second
	b.Reset()
	return b
}
