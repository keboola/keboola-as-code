package backoff

import (
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/jonboulle/clockwork"
)

// TimeBackoff is a BackOff implementation that limits the total elapsed time.
// This functionality was present in backoff v4 but removed in backoff v5.
type TimeBackoff struct {
	backoff.BackOff
	MaxElapsedTime time.Duration
	Clock          clockwork.Clock
	startTime      time.Time
}

func NewTimeBackoff(b backoff.BackOff, maxElapsedTime time.Duration) *TimeBackoff {
	tb := &TimeBackoff{
		BackOff:        b,
		MaxElapsedTime: maxElapsedTime,
		Clock:          clockwork.NewRealClock(),
	}
	tb.Reset()
	return tb
}

func (b *TimeBackoff) NextBackOff() time.Duration {
	next := b.BackOff.NextBackOff()

	if b.MaxElapsedTime != 0 {
		elapsed := b.GetElapsedTime()
		if elapsed+next >= b.MaxElapsedTime {
			return backoff.Stop
		}
	}

	return next
}

func (b *TimeBackoff) Reset() {
	b.startTime = b.Clock.Now()
	b.BackOff.Reset()
}

func (b *TimeBackoff) GetElapsedTime() time.Duration {
	return b.Clock.Now().Sub(b.startTime)
}
