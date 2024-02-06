package service

import (
	"math"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
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

func calculateFileRetryTime(f *model.File, now time.Time) *utctime.UTCTime {
	attempt := f.RetryAttempt + 1
	retryAfter := utctime.UTCTime(RetryAt(NewRetryBackoff(), now, attempt))
	f.RetryAttempt = attempt
	f.RetryAfter = &retryAfter
	return f.RetryAfter
}
