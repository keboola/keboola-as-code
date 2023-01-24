package service

import (
	"math"
	"time"

	"github.com/cenkalti/backoff/v4"
)

func NewRetryBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0.2
	b.Multiplier = 4
	b.InitialInterval = 2 * time.Minute
	b.MaxInterval = 3 * time.Hour
	b.MaxElapsedTime = 0 // don't stop
	b.Reset()
	return b
}

func RetryAt(b *backoff.ExponentialBackOff, now time.Time, attempt int) time.Time {
	max := float64(b.MaxInterval)
	interval := b.InitialInterval
	total := interval
	for i := 0; i < attempt-1; i++ {
		interval = time.Duration(math.Min(float64(interval)*b.Multiplier, max))
		total += interval
	}
	return now.Add(total)
}
