package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testvalidation"
)

func TestRetryable_Allowed(t *testing.T) {
	t.Parallel()

	now := utctime.MustParse("2000-01-01T01:00:00.000Z").Time()

	v := Retryable{}
	assert.True(t, v.Allowed(now))

	timeInPast := utctime.From(now.Add(-time.Second))
	v.RetryAttempt = 1
	v.RetryAfter = &timeInPast
	assert.True(t, v.Allowed(now))

	timeInFuture := utctime.From(now.Add(time.Second))
	v.RetryAttempt = 1
	v.RetryAfter = &timeInFuture
	assert.False(t, v.Allowed(now))
}

func TestRetryable_Validation(t *testing.T) {
	t.Parallel()

	cases := testvalidation.TestCases[Retryable]{
		{
			Name:  "empty",
			Value: Retryable{},
		},
		{
			Name: "attempt=0, unexpected fields",
			ExpectedError: `
- "retryReason" should not be set
- "firstFailedAt" should not be set
- "lastFailedAt" should not be set
- "retryAfter" should not be set`,
			Value: Retryable{
				RetryAttempt:  0,
				RetryReason:   "foo",
				FirstFailedAt: ptr.Ptr(utctime.MustParse("2000-01-01T15:00:00.000Z")),
				LastFailedAt:  ptr.Ptr(utctime.MustParse("2000-01-01T15:00:00.000Z")),
				RetryAfter:    ptr.Ptr(utctime.MustParse("2000-01-01T17:00:00.000Z")),
			},
		},
		{
			Name: "attempt=1, missing fields",
			ExpectedError: `
- "retryReason" is a required field
- "firstFailedAt" is a required field
- "lastFailedAt" is a required field
- "retryAfter" is a required field`,
			Value: Retryable{
				RetryAttempt: 1,
			},
		},
		{
			Name: "attempt=1, ok",
			Value: Retryable{
				RetryAttempt:  1,
				RetryReason:   "foo",
				FirstFailedAt: ptr.Ptr(utctime.MustParse("2000-01-01T15:00:00.000Z")),
				LastFailedAt:  ptr.Ptr(utctime.MustParse("2000-01-01T15:00:00.000Z")),
				RetryAfter:    ptr.Ptr(utctime.MustParse("2000-01-01T17:00:00.000Z")),
			},
		},
		{
			Name:          "LastFailedAt < FirstFailedAt",
			ExpectedError: `"lastFailedAt" must be greater than or equal to FirstFailedAt`,
			Value: Retryable{
				RetryAttempt:  1,
				RetryReason:   "foo",
				FirstFailedAt: ptr.Ptr(utctime.MustParse("2000-01-01T17:00:00.000Z")),
				LastFailedAt:  ptr.Ptr(utctime.MustParse("2000-01-01T16:00:00.000Z")),
				RetryAfter:    ptr.Ptr(utctime.MustParse("2000-01-01T18:00:00.000Z")),
			},
		},
		{
			Name:          "RetryAfter < FirstFailedAt",
			ExpectedError: `"retryAfter" must be greater than or equal to FirstFailedAt`,
			Value: Retryable{
				RetryAttempt:  1,
				RetryReason:   "foo",
				FirstFailedAt: ptr.Ptr(utctime.MustParse("2000-01-01T17:00:00.000Z")),
				LastFailedAt:  ptr.Ptr(utctime.MustParse("2000-01-01T17:00:00.000Z")),
				RetryAfter:    ptr.Ptr(utctime.MustParse("2000-01-01T15:00:00.000Z")),
			},
		},
		{
			Name:          "RetryAfter < LastFailedAt",
			ExpectedError: `"retryAfter" must be greater than or equal to LastFailedAt`,
			Value: Retryable{
				RetryAttempt:  1,
				RetryReason:   "foo",
				FirstFailedAt: ptr.Ptr(utctime.MustParse("2000-01-01T10:00:00.000Z")),
				LastFailedAt:  ptr.Ptr(utctime.MustParse("2000-01-01T17:00:00.000Z")),
				RetryAfter:    ptr.Ptr(utctime.MustParse("2000-01-01T15:00:00.000Z")),
			},
		},
	}

	// Run test cases
	cases.Run(t)
}

func TestRetryable_IncrementRetry(t *testing.T) {
	t.Parallel()

	backoff := NoRandomizationBackoff()
	backoff.(*retryBackoff).RandomizationFactor = 0

	v := Retryable{}

	// 1
	v.IncrementRetryAttempt(backoff, utctime.MustParse("2000-01-01T00:00:00.000Z").Time(), "some reason")
	assert.Equal(t, Retryable{
		RetryAttempt:  1,
		RetryReason:   "some reason",
		FirstFailedAt: ptr.Ptr(utctime.MustParse("2000-01-01T00:00:00.000Z")),
		LastFailedAt:  ptr.Ptr(utctime.MustParse("2000-01-01T00:00:00.000Z")),
		RetryAfter:    ptr.Ptr(utctime.MustParse("2000-01-01T00:02:00.000Z")), // +2 min
	}, v)

	// 2
	v.IncrementRetryAttempt(backoff, utctime.MustParse("2000-01-01T01:00:00.000Z").Time(), "some reason")
	assert.Equal(t, Retryable{
		RetryAttempt:  2,
		RetryReason:   "some reason",
		FirstFailedAt: ptr.Ptr(utctime.MustParse("2000-01-01T00:00:00.000Z")),
		LastFailedAt:  ptr.Ptr(utctime.MustParse("2000-01-01T01:00:00.000Z")),
		RetryAfter:    ptr.Ptr(utctime.MustParse("2000-01-01T01:08:00.000Z")), // +8 min
	}, v)

	// 3
	v.IncrementRetryAttempt(backoff, utctime.MustParse("2000-01-01T02:00:00.000Z").Time(), "some reason")
	assert.Equal(t, Retryable{
		RetryAttempt:  3,
		RetryReason:   "some reason",
		FirstFailedAt: ptr.Ptr(utctime.MustParse("2000-01-01T00:00:00.000Z")),
		LastFailedAt:  ptr.Ptr(utctime.MustParse("2000-01-01T02:00:00.000Z")),
		RetryAfter:    ptr.Ptr(utctime.MustParse("2000-01-01T02:32:00.000Z")), // +32 min
	}, v)
}

func TestRetryable_ResetRetry(t *testing.T) {
	t.Parallel()

	v := Retryable{
		RetryAttempt:  1,
		RetryReason:   "foo",
		FirstFailedAt: ptr.Ptr(utctime.MustParse("2000-01-01T15:00:00.000Z")),
		LastFailedAt:  ptr.Ptr(utctime.MustParse("2000-01-01T15:00:00.000Z")),
		RetryAfter:    ptr.Ptr(utctime.MustParse("2000-01-01T17:00:00.000Z")),
	}

	v.ResetRetry()

	assert.Equal(t, Retryable{
		RetryAttempt:  0,
		RetryReason:   "",
		FirstFailedAt: nil,
		LastFailedAt:  nil,
		RetryAfter:    nil,
	}, v)
}

func TestRetryBackoff_RetryAt_Stable(t *testing.T) {
	t.Parallel()

	backoff := NoRandomizationBackoff()
	now := utctime.MustParse("2000-01-01T00:00:00.000Z").Time()

	assert.Panics(t, func() {
		backoff.RetryAt(now, -1)
	})

	assert.Panics(t, func() {
		backoff.RetryAt(now, 0)
	})

	// Assert static delays
	expected := []string{
		"2000-01-01T00:02:00.000Z", // +2 min
		"2000-01-01T00:10:00.000Z", // +8 min (x4)
		"2000-01-01T00:42:00.000Z", // +32 min (x4)
		"2000-01-01T02:50:00.000Z", // +128 min (x4)
		"2000-01-01T05:50:00.000Z", // +3h (max)
		"2000-01-01T08:50:00.000Z", // +3h
		"2000-01-01T11:50:00.000Z", // +3h
	}

	now = utctime.MustParse("2000-01-01T00:00:00.000Z").Time()
	for i, e := range expected {
		retryAt := backoff.RetryAt(now, i+1)
		assert.Equal(t, e, utctime.From(retryAt).String())
		now = retryAt
	}
}

func TestRetryBackoff_RetryAt_Random(t *testing.T) {
	t.Parallel()

	backoff := DefaultBackoff()
	now := utctime.MustParse("2000-01-01T00:00:00.000Z").Time()

	assert.Panics(t, func() {
		backoff.RetryAt(now, -1)
	})

	assert.Panics(t, func() {
		backoff.RetryAt(now, 0)
	})

	// Assert randomized delays, the failedAt value is used as a random seed, so results are stable
	expected := []string{
		"2000-01-01T00:01:50.098Z", // +2 min
		"2000-01-01T00:09:25.554Z", // +8 min (x4)
		"2000-01-01T00:38:31.057Z", // +32 min (x4)
		"2000-01-01T02:41:28.265Z", // +128 min (x4)
		"2000-01-01T05:34:27.609Z", // +3h (max)
		"2000-01-01T08:19:13.994Z", // +3h
		"2000-01-01T11:12:22.930Z", // +3h
	}

	now = utctime.MustParse("2000-01-01T00:00:00.000Z").Time()
	for i, e := range expected {
		retryAt := backoff.RetryAt(now, i+1)
		assert.Equal(t, e, utctime.From(retryAt).String())
		now = retryAt
	}
}

func TestRetryable_IncrementNonRetryableAttempt(t *testing.T) {
	t.Parallel()

	fixedInterval := 2 * time.Hour
	v := Retryable{}

	// First attempt
	v.IncrementNonRetryableAttempt(utctime.MustParse("2000-01-01T00:00:00.000Z").Time(), "credential expired", fixedInterval)
	assert.Equal(t, Retryable{
		RetryAttempt:  1,
		RetryReason:   "credential expired",
		FirstFailedAt: ptr.Ptr(utctime.MustParse("2000-01-01T00:00:00.000Z")),
		LastFailedAt:  ptr.Ptr(utctime.MustParse("2000-01-01T00:00:00.000Z")),
		RetryAfter:    ptr.Ptr(utctime.MustParse("2000-01-01T02:00:00.000Z")), // +2 hours
	}, v)

	// Second attempt - FirstFailedAt should remain unchanged
	v.IncrementNonRetryableAttempt(utctime.MustParse("2000-01-01T02:00:00.000Z").Time(), "credential expired", fixedInterval)
	assert.Equal(t, Retryable{
		RetryAttempt:  2,
		RetryReason:   "credential expired",
		FirstFailedAt: ptr.Ptr(utctime.MustParse("2000-01-01T00:00:00.000Z")), // unchanged
		LastFailedAt:  ptr.Ptr(utctime.MustParse("2000-01-01T02:00:00.000Z")),
		RetryAfter:    ptr.Ptr(utctime.MustParse("2000-01-01T04:00:00.000Z")), // +2 hours (fixed)
	}, v)

	// Third attempt - still fixed interval
	v.IncrementNonRetryableAttempt(utctime.MustParse("2000-01-01T04:00:00.000Z").Time(), "credential expired", fixedInterval)
	assert.Equal(t, Retryable{
		RetryAttempt:  3,
		RetryReason:   "credential expired",
		FirstFailedAt: ptr.Ptr(utctime.MustParse("2000-01-01T00:00:00.000Z")), // unchanged
		LastFailedAt:  ptr.Ptr(utctime.MustParse("2000-01-01T04:00:00.000Z")),
		RetryAfter:    ptr.Ptr(utctime.MustParse("2000-01-01T06:00:00.000Z")), // +2 hours (fixed)
	}, v)
}

func TestRetryable_IncrementNonRetryableAttempt_VsExponential(t *testing.T) {
	t.Parallel()

	// This test demonstrates the difference between exponential backoff (IncrementRetryAttempt)
	// and fixed interval (IncrementNonRetryableAttempt)

	backoff := NoRandomizationBackoff()
	fixedInterval := 2 * time.Hour

	exponential := Retryable{}
	fixed := Retryable{}

	baseTime := utctime.MustParse("2000-01-01T00:00:00.000Z").Time()

	// First attempt - both use their base interval
	exponential.IncrementRetryAttempt(backoff, baseTime, "retryable error")
	fixed.IncrementNonRetryableAttempt(baseTime, "non-retryable error", fixedInterval)

	assert.Equal(t, "2000-01-01T00:02:00.000Z", exponential.RetryAfter.String()) // +2 min (exponential base)
	assert.Equal(t, "2000-01-01T02:00:00.000Z", fixed.RetryAfter.String())       // +2 hours (fixed)

	// Second attempt - exponential increases, fixed stays same
	exponential.IncrementRetryAttempt(backoff, baseTime.Add(2*time.Minute), "retryable error")
	fixed.IncrementNonRetryableAttempt(baseTime.Add(2*time.Hour), "non-retryable error", fixedInterval)

	assert.Equal(t, "2000-01-01T00:10:00.000Z", exponential.RetryAfter.String()) // +8 min (exponential x4)
	assert.Equal(t, "2000-01-01T04:00:00.000Z", fixed.RetryAfter.String())       // +2 hours (fixed)

	// Third attempt - exponential continues to grow, fixed stays same
	exponential.IncrementRetryAttempt(backoff, baseTime.Add(10*time.Minute), "retryable error")
	fixed.IncrementNonRetryableAttempt(baseTime.Add(4*time.Hour), "non-retryable error", fixedInterval)

	assert.Equal(t, "2000-01-01T00:42:00.000Z", exponential.RetryAfter.String()) // +32 min (exponential x4)
	assert.Equal(t, "2000-01-01T06:00:00.000Z", fixed.RetryAfter.String())       // +2 hours (fixed)

	// Verify FirstFailedAt remains unchanged for both
	assert.Equal(t, baseTime, exponential.FirstFailedAt.Time())
	assert.Equal(t, baseTime, fixed.FirstFailedAt.Time())
}

func TestRetryable_IncrementNonRetryableAttempt_TerminalInterval(t *testing.T) {
	t.Parallel()

	// Test with a very long "terminal" interval (simulating max attempts reached)
	terminalInterval := 10 * 365 * 24 * time.Hour // ~10 years
	v := Retryable{}

	baseTime := utctime.MustParse("2000-01-01T00:00:00.000Z").Time()
	v.IncrementNonRetryableAttempt(baseTime, "max attempts reached", terminalInterval)

	// Verify far-future retry time (should be at least 9 years in the future)
	expectedRetryAfter := utctime.From(baseTime.Add(terminalInterval))
	assert.Equal(t, expectedRetryAfter.String(), v.RetryAfter.String())

	// Verify it's far in the future (more than 9 years)
	nineYearsLater := baseTime.Add(9 * 365 * 24 * time.Hour)
	assert.True(t, v.RetryAfter.Time().After(nineYearsLater), "RetryAfter should be more than 9 years in the future")
}
