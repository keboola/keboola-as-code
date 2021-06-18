package remote

import (
	"github.com/cenkalti/backoff/v4"
	"time"
)

// newBackoff for checking Job status and similar operations
func newBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0
	b.InitialInterval = 50 * time.Millisecond
	b.Multiplier = 2
	b.MaxInterval = 3 * time.Second
	b.MaxElapsedTime = 30 * time.Second
	b.Reset()
	return b
}
