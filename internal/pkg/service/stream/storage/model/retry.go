package model

import (
	"math"
	"math/rand"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Retryable struct {
	RetryAttempt  int              `json:"retryAttempt,omitempty"`
	RetryReason   string           `json:"retryReason,omitempty" validate:"required_with=RetryAttempt,excluded_without=RetryAttempt"`
	FirstFailedAt *utctime.UTCTime `json:"firstFailedAt,omitempty" validate:"required_with=RetryAttempt,excluded_without=RetryAttempt"`
	LastFailedAt  *utctime.UTCTime `json:"lastFailedAt,omitempty" validate:"required_with=RetryAttempt,excluded_without=RetryAttempt,gtefield=FirstFailedAt"`
	RetryAfter    *utctime.UTCTime `json:"retryAfter,omitempty" validate:"required_with=RetryAttempt,excluded_without=RetryAttempt,gtefield=FirstFailedAt,gtefield=LastFailedAt"`
}

// RetryBackoff determines the time in the future after which a failed operation will be retried.
// Unlike other backoffs, it does not generate a delay but a target time.
type RetryBackoff interface {
	RetryAt(failedAt time.Time, attempt int) (retryAt time.Time)
}

// retryBackoff implements RetryBackoff.
type retryBackoff struct {
	*backoff.ExponentialBackOff
}

func NewBackoff(wrapped *backoff.ExponentialBackOff) RetryBackoff {
	return &retryBackoff{ExponentialBackOff: wrapped}
}

func DefaultBackoff() RetryBackoff {
	b := newBackoff()
	b.Reset()
	return NewBackoff(b)
}

func NoRandomizationBackoff() RetryBackoff {
	b := newBackoff()
	b.RandomizationFactor = 0
	b.Reset()
	return NewBackoff(b)
}

func newBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0.1
	b.Multiplier = 4
	b.InitialInterval = 2 * time.Minute
	b.MaxInterval = 3 * time.Hour
	b.MaxElapsedTime = 0 // don't stop
	b.Reset()
	return b
}

func (b *retryBackoff) RetryAt(failedAt time.Time, attempt int) (retryAt time.Time) {
	if attempt <= 0 {
		panic(errors.New("attempt must be greater than 0"))
	}

	interval := time.Duration(
		math.Min(
			float64(b.InitialInterval)*math.Pow(b.Multiplier, float64(attempt-1)),
			float64(b.MaxInterval),
		),
	)

	random := rand.New(rand.NewSource(failedAt.UnixNano()))                              //nolint:gosec // week random generator is ok here
	randomFactor := 1 - b.RandomizationFactor + random.Float64()*2*b.RandomizationFactor // 1 ± RandomizationFactor

	return failedAt.Add(time.Duration(float64(interval) * randomFactor))
}

func (v *Retryable) Allowed(now time.Time) bool {
	return v.RetryAttempt == 0 || v.RetryAfter.Time().Before(now)
}

func (v *Retryable) IncrementRetryAttempt(backoff RetryBackoff, failedAt time.Time, reason string) {
	v.RetryAttempt += 1
	v.RetryReason = reason

	failedAtUTC := utctime.From(failedAt)
	if v.FirstFailedAt == nil {
		v.FirstFailedAt = &failedAtUTC
	}
	v.LastFailedAt = &failedAtUTC

	retryAfterUTC := utctime.From(backoff.RetryAt(failedAt, v.RetryAttempt))
	v.RetryAfter = &retryAfterUTC
}

func (v *Retryable) ResetRetry() {
	v.RetryAttempt = 0
	v.RetryReason = ""
	v.FirstFailedAt = nil
	v.LastFailedAt = nil
	v.RetryAfter = nil
}
