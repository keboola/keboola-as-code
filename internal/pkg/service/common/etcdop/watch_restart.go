package etcdop

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type RestartableWatchStream[E WatchEvent] interface {
	Channel() <-chan WatchResponseE[E]
	Restart(cause error)
}

// RestartableWatchStreamRaw is restarted on a fatal error, or manually by the Restart method.
type RestartableWatchStreamRaw struct {
	WatchStreamE[WatchEventRaw]
	lock *sync.Mutex
	sub  *WatchStreamE[WatchEventRaw]
}

// Restart cancels the current stream, so a new stream is created.
func (s *RestartableWatchStreamRaw) Restart(cause error) {
	s.lock.Lock()
	if s.sub != nil {
		s.sub.cancelCause = cause
		s.sub.cancel(cause)
		for range s.sub.channel {
			// wait for the channel close
		}
	}
	s.lock.Unlock()
}

func (s *RestartableWatchStreamRaw) SetupConsumer() WatchConsumerSetup[WatchEventRaw] {
	return newConsumerSetup((RestartableWatchStream[WatchEventRaw])(s))
}

// wrapStreamWithRestart continuously tries to restart the stream on fatal errors.
// An exponential backoff is used between attempts.
// The operation can be cancelled using the context.
func wrapStreamWithRestart(ctx context.Context, channelFactory func(ctx context.Context) *WatchStreamRaw) *RestartableWatchStreamRaw {
	b := backoff.WithContext(newWatchBackoff(), ctx)
	ctx, cancel := context.WithCancelCause(ctx)
	stream := &RestartableWatchStreamRaw{WatchStreamE: WatchStreamRaw{channel: make(chan WatchResponseRaw), cancel: cancel}, lock: &sync.Mutex{}}
	go func() {
		defer close(stream.channel)
		defer cancel(context.Canceled)

		// The initialization phase lasts until the first "created" event.
		// If the watch operation was restarted,
		// the next initialization error is converted to a common error.
		init := true

		// The "restarted" event contains RestartCause - last error.
		var lastErr error
		var restart bool
		var restartCause error
		var restartDelay time.Duration

		for {
			// Is context done?
			if ctx.Err() != nil {
				return
			}

			// Store reference to the current stream, so it can be canceled/restarted
			subStream := channelFactory(ctx)
			stream.lock.Lock()
			stream.sub = subStream
			stream.lock.Unlock()

			for resp := range subStream.channel {
				// Emit "restarted" event before the first event after the restart
				if restart {
					var cause error
					switch {
					case lastErr != nil:
						cause = errors.PrefixErrorf(lastErr, `unexpected restart, backoff delay %s, cause:`, restartDelay)
					case restartCause != nil:
						cause = restartCause
					default:
						cause = errors.New("unknown cause") // shouldn't happen
					}

					rst := WatchResponseRaw{}
					rst.Restarted = true
					rst.RestartCause = cause
					rst.RestartDelay = restartDelay
					stream.channel <- rst
					lastErr = nil
					restart = false
					restartCause = nil
				}

				// Pass OnCreated to the stream.channel channel
				if resp.Created {
					init = false
					b.Reset()
				}

				// Update lastErr for RestartCause
				if resp.InitErr != nil {
					lastErr = resp.InitErr
				} else if resp.Err != nil {
					lastErr = resp.Err
				}

				// Handle initialization error
				if err := resp.InitErr; err != nil {
					if init { // nolint: gocritic
						// Stop on initialization error
						stream.channel <- resp
						return
					} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						// Context cancelled event is forwarded only during the initialization.
						// In other cases, closing the channel is sufficient.
						continue
					} else {
						// Convert initialization error
						// from an 1+ attempt to a common error and restart watch.
						resp.Err = err
						resp.InitErr = nil
						stream.channel <- resp
						break
					}
				}

				// Pass the response
				stream.channel <- resp
			}

			// Restart is in progress
			restart = true

			// Delay is applied only if the restart is caused by an error, not by the manual restart
			var delay time.Duration
			if lastErr != nil {
				// Calculate delay
				restartDelay = b.NextBackOff()
				if restartDelay == backoff.Stop {
					return
				}

				// Wait before restart
				select {
				case <-ctx.Done():
					return
				case <-time.After(delay):
					// continue
				}
			} else if subStream.cancelCause != nil {
				// If the stream was cancelled manually (without error), save the cancellation reason
				restartCause = subStream.cancelCause
			}
		}
	}()

	return stream
}

func newWatchBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0.2
	b.InitialInterval = 50 * time.Millisecond
	b.Multiplier = 2
	b.MaxInterval = 1 * time.Minute
	b.MaxElapsedTime = 0 // never stop
	b.Reset()
	return b
}
