package service_test

import (
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/service"
)

func TestRetryBackoff(t *testing.T) {
	t.Parallel()

	b := service.NewRetryBackoff()
	b.RandomizationFactor = 0

	clk := clock.NewMock()
	b.Clock = clk
	b.Reset()

	// Get all delays without sleep
	var delays []time.Duration
	for i := 0; i < 7; i++ {
		delay := b.NextBackOff()
		if delay == backoff.Stop {
			assert.Fail(t, "received unexpected stop")
		}
		delays = append(delays, delay)
		clk.Add(delay)
	}

	// Assert
	assert.Equal(t, []time.Duration{
		2 * time.Minute,
		8 * time.Minute,
		32 * time.Minute,
		128 * time.Minute,
		3 * time.Hour,
		3 * time.Hour,
		3 * time.Hour,
	}, delays)
}

func TestRetryAt(t *testing.T) {
	t.Parallel()

	b := service.NewRetryBackoff()
	b.RandomizationFactor = 0
	now, _ := time.Parse(time.RFC3339, "2010-01-01T00:00:00Z")
	assert.Equal(t, "2010-01-01T00:02:00Z", service.RetryAt(b, now, 1).Format(time.RFC3339))
	assert.Equal(t, "2010-01-01T00:10:00Z", service.RetryAt(b, now, 2).Format(time.RFC3339))
	assert.Equal(t, "2010-01-01T00:42:00Z", service.RetryAt(b, now, 3).Format(time.RFC3339)) // 2 + 8 + 32 = 42
	assert.Equal(t, "2010-01-01T02:50:00Z", service.RetryAt(b, now, 4).Format(time.RFC3339)) // ...
	assert.Equal(t, "2010-01-01T05:50:00Z", service.RetryAt(b, now, 5).Format(time.RFC3339))
	assert.Equal(t, "2010-01-01T08:50:00Z", service.RetryAt(b, now, 6).Format(time.RFC3339))
	assert.Equal(t, "2010-01-01T11:50:00Z", service.RetryAt(b, now, 7).Format(time.RFC3339))
}
