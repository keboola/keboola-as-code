package encoding

import (
	"time"

	"github.com/cenkalti/backoff/v5"
)

// maxChunkRetryDuration bounds how long processChunks keeps retrying writes to a broken
// network output before giving up and closing the pipeline. Without a bound, a pipeline
// stuck on a permanently unreachable volume retries forever, leaking its goroutine and
// buffers instead of being recreated.
const maxChunkRetryDuration = 5 * time.Minute

func newChunkBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0.1
	b.Multiplier = 2
	b.InitialInterval = 100 * time.Millisecond
	b.MaxInterval = 15 * time.Second
	b.Reset()
	return b
}
