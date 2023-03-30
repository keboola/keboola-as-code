package orchestrator

import (
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
)

func TestRetryBackoff(t *testing.T) {
	t.Parallel()

	b := newRetryBackoff()
	b.RandomizationFactor = 0

	clk := clock.NewMock()
	b.Clock = clk
	b.Reset()

	// Get all delays without sleep
	var delays []time.Duration
	for i := 0; i < 11; i++ {
		delay := b.NextBackOff()
		if delay == backoff.Stop {
			assert.Fail(t, "received unexpected stop")
		}
		delays = append(delays, delay)
		clk.Add(delay)
	}

	// Assert
	assert.Equal(t, []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		3200 * time.Millisecond,
		6400 * time.Millisecond,
		12800 * time.Millisecond,
		25600 * time.Millisecond,
		30 * time.Second,
		30 * time.Second,
	}, delays)
}
