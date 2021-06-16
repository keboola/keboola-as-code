package api

import (
	"github.com/cenkalti/backoff/v4"
	"time"
)

// createBackoff for checking Job status and similar operations
func (a *StorageApi) createBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 100 * time.Millisecond
	b.Multiplier = 2
	b.MaxInterval = 3 * time.Second
	b.MaxElapsedTime = 30 * time.Second
	return b
}
