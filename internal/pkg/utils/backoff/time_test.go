package backoff

import (
	"testing"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
)

func TestTimeBackoff(t *testing.T) {
	t.Parallel()

	clk := clockwork.NewFakeClock()

	eb := backoff.NewExponentialBackOff()
	eb.RandomizationFactor = 0
	eb.Multiplier = 2
	eb.InitialInterval = 10 * time.Millisecond
	eb.MaxInterval = 2 * time.Second
	b := NewTimeBackoff(eb, 15 * time.Second)
	b.Clock = clk
	b.Reset()

	// Get all delays without sleep
	var delays []time.Duration
	for {
		delay := b.NextBackOff()
		if delay == backoff.Stop {
			break
		}
		delays = append(delays, delay)
		clk.Advance(delay)
	}

	// Assert
	assert.Equal(t, []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		40 * time.Millisecond,
		80 * time.Millisecond,
		160 * time.Millisecond,
		320 * time.Millisecond,
		640 * time.Millisecond,
		1280 * time.Millisecond,
		2000 * time.Millisecond,
		2000 * time.Millisecond,
		2000 * time.Millisecond,
		2000 * time.Millisecond,
		2000 * time.Millisecond,
		2000 * time.Millisecond,
	}, delays)
}
