package remote

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type TestClock struct {
	now time.Time
}

func (c *TestClock) Now() time.Time {
	return c.now
}

func (c *TestClock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}

func TestBackoff(t *testing.T) {
	clock := &TestClock{now: time.Now()}
	backoff := newBackoff()
	backoff.Clock = clock

	// Get all delays without sleep
	var delays []time.Duration
	for {
		delay := backoff.NextBackOff()
		if delay == backoff.Stop {
			break
		}
		delays = append(delays, delay)
		clock.Advance(delay)
	}

	// Assert
	assert.Equal(t, []time.Duration{
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		3000 * time.Millisecond,
		3000 * time.Millisecond,
		3000 * time.Millisecond,
		3000 * time.Millisecond,
		3000 * time.Millisecond,
		3000 * time.Millisecond,
		3000 * time.Millisecond,
		3000 * time.Millisecond,
	}, delays)
}
