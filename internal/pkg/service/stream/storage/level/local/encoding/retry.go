package encoding

import (
	"time"

	"github.com/cenkalti/backoff/v5"
)

func newChunkBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0.1
	b.Multiplier = 2
	b.InitialInterval = 100 * time.Millisecond
	b.MaxInterval = 15 * time.Second
	b.Reset()
	return b
}

// defaultChunkRetryDuration is the give-up window used in place of a non-positive
// encoding.Config.MaxChunkRetryDuration.
//
// Config validation requires minDuration=1s, so an explicit zero can never reach here through
// config load or a Sink config patch. It can only happen because File/Slice records are persisted
// with this field baked in, and the etcd serde's validate closure skips struct/pointer values on
// decode - so a record written before this field existed decodes it as zero. Without this floor,
// that zero would mean "give up after the first backoff retry" instead of the intended default.
const defaultChunkRetryDuration = 5 * time.Minute

// chunkRetryDuration floors a configured give-up window to defaultChunkRetryDuration when it is
// zero or negative. See defaultChunkRetryDuration for why that can happen.
func chunkRetryDuration(configured time.Duration) time.Duration {
	if configured <= 0 {
		return defaultChunkRetryDuration
	}
	return configured
}
