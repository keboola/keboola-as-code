package etcdop

import (
	"context"
	"sync"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// WatchConsumer simplifies handling of watch events.
// It can be created by the WatchStreamE.SetupConsumer.
// Then it should be configured using the With* methods.
// At the end, the StartConsumer method should be called.
//
// The StartConsumer method returns an initialization error channel.
// After initialization, an error is written there OR the channel is closed without error.
// It is necessary to wait for the channel and check whether an error has occurred.
// If the error occurs, the watcher stops.
// The channel is returned and not directly the error,
// because you can wait for the initialization of several watchers in parallel.
//
// After initialization, all errors are retried, so no error can terminate the watcher.
// If a fatal error occurs, the entire Watch/GetAllAndWatch operation is restarted.
// In that case, the "restarted" event is emitted, see WithOnRestarted method.
// For the first batch of events after the "restart",
// the ForEach callback is called with the "restart=true" flag, see WithForEach method.
//
// The WatchConsumer can be canceled by cancelling the context passed to the Watch/GetAllAndWatch method.
type WatchConsumer[E WatchEvent] struct {
	stream      RestartableWatchStream[E]
	forEachFn   func(events []E, header *Header, restart bool)
	onCreated   onWatcherCreated
	onRestarted onWatcherRestarted
	onError     onWatcherError
	onClose     onWatcherClose
}

type WatchConsumerSetup[E WatchEvent] struct {
	stream      RestartableWatchStream[E]
	forEachFn   func(events []E, header *Header, restart bool)
	onCreated   onWatcherCreated
	onRestarted onWatcherRestarted
	onError     onWatcherError
	onClose     onWatcherClose
}

type Restartable interface {
	Restart(cause error)
}

type (
	onWatcherCreated   func(header *Header)
	onWatcherRestarted func(cause error, delay time.Duration)
	onWatcherError     func(err error)
	onWatcherClose     func(err error)
)

func newConsumerSetup[E WatchEvent](stream RestartableWatchStream[E]) WatchConsumerSetup[E] {
	return WatchConsumerSetup[E]{stream: stream}
}

// Restart underlying stream.
func (c *WatchConsumer[E]) Restart(cause error) {
	c.stream.Restart(cause)
}

func (s WatchConsumerSetup[E]) WithForEach(v func(events []E, header *Header, restart bool)) WatchConsumerSetup[E] {
	s.forEachFn = v
	return s
}

func (s WatchConsumerSetup[E]) WithOnCreated(v onWatcherCreated) WatchConsumerSetup[E] {
	s.onCreated = v
	return s
}

func (s WatchConsumerSetup[E]) WithOnRestarted(v onWatcherRestarted) WatchConsumerSetup[E] {
	s.onRestarted = v
	return s
}

func (s WatchConsumerSetup[E]) WithOnError(v onWatcherError) WatchConsumerSetup[E] {
	s.onError = v
	return s
}

func (s WatchConsumerSetup[E]) WithOnClose(v onWatcherClose) WatchConsumerSetup[E] {
	s.onClose = v
	return s
}

func (s WatchConsumerSetup[E]) StartConsumer(ctx context.Context, wg *sync.WaitGroup, logger log.Logger) (consumer *WatchConsumer[E], initErr <-chan error) {
	// Copy settings, they are immutable after the consumer start
	c := &WatchConsumer[E]{
		stream:      s.stream,
		forEachFn:   s.forEachFn,
		onCreated:   s.onCreated,
		onRestarted: s.onRestarted,
		onError:     s.onError,
		onClose:     s.onClose,
	}

	initErrCh := make(chan error, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()

		init := initErrCh

		// The flag restart=true is send with the first events batch after the "restarted" event, see WatchConsumer.forEachFn.
		restart := false

		// See watchErrorThreshold
		var lastErrorAt time.Time
		var lastError error

		// Channel is closed when the watcher context is cancelled,
		// so the context does not have to be checked here.
		for resp := range c.stream.Channel() {
			switch {
			case resp.InitErr != nil:
				// Initialization error, the channel will be closed in the beginning of the next iteration.
				// Signal the problem via InitErr channel.
				// It is fatal error (e.g., no network connection), the app should be stopped and restarted.
				init <- resp.InitErr
				close(init)
				init = nil
			case resp.Err != nil:
				// An error occurred, it is logged.
				// If it is a fatal error, then it is followed
				// by the "Restarted" event handled bellow,
				// and the operation starts from the beginning.
				//
				// ErrCompacted or ErrLeaderChanged occurs even during cluster normal operation,
				// so the error is logged with warning log level.
				//
				// It is suspicious if a short time has passed between two errors,
				// then the error is logged with error log level.
				if interval := time.Since(lastErrorAt); interval > watchErrorThreshold {
					logger.Warn(ctx, resp.Err.Error())
				} else {
					logger.Error(ctx, errors.Errorf(`%s (previous error %s ago)`, resp.Err, interval).Error())
				}
				lastErrorAt = time.Now()
				lastError = resp.Err
				if c.onError != nil {
					c.onError(resp.Err)
				}
			case resp.Restarted:
				// A fatal error (etcd ErrCompacted) occurred.
				// It is not possible to continue watching, the operation must be restarted.
				restart = true
				logger.Infof(ctx, "watch stream consumer restarted: %s", resp.RestartCause)
				if c.onRestarted != nil {
					c.onRestarted(resp.RestartCause, resp.RestartDelay)
				}
			case resp.Created:
				// The watcher has been successfully created.
				// This means transition from GetAll to Watch phase.
				// The Created event is emitted always if a new watches is created, so after initialization and each restart.
				logger.Info(ctx, "watcher created")
				if c.onCreated != nil {
					c.onCreated(resp.Header)
				}
				if init != nil {
					close(init)
					init = nil
				}
			default:
				lastError = nil
				c.forEachFn(resp.Events, resp.Header, restart)
				restart = false
			}
		}

		// Close
		var closeErr error
		if lastError != nil {
			closeErr = lastError
		} else if err := ctx.Err(); err != nil {
			closeErr = err
		} else {
			closeErr = errors.New("unknown cause") // shouldn't happen
		}
		logger.Infof(ctx, "watch stream consumer closed: %s", closeErr.Error())
		if c.onClose != nil {
			c.onClose(closeErr)
		}
	}()

	return c, initErrCh
}
