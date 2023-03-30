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
type WatchConsumer[E any] struct {
	logger      log.Logger
	stream      <-chan WatchResponseE[E]
	forEachFn   func(events []E, header *Header, restart bool)
	onCreated   onWatcherCreated
	onRestarted onWatcherRestarted
	onError     onWatcherError
	onClose     onWatcherClose
}

type WatchStreamE[E any] <-chan WatchResponseE[E]

type (
	onWatcherCreated   func(header *Header)
	onWatcherRestarted func(reason string, delay time.Duration)
	onWatcherError     func(err error)
	onWatcherClose     func(err error)
)

func newConsumer[E any](logger log.Logger, stream <-chan WatchResponseE[E]) WatchConsumer[E] {
	return WatchConsumer[E]{
		logger: logger,
		stream: stream,
	}
}

func (s WatchStreamE[E]) SetupConsumer(logger log.Logger) WatchConsumer[E] {
	return newConsumer[E](logger, s)
}

func (c WatchConsumer[E]) WithForEach(v func(events []E, header *Header, restart bool)) WatchConsumer[E] {
	c.forEachFn = v
	return c
}

func (c WatchConsumer[E]) WithOnCreated(v onWatcherCreated) WatchConsumer[E] {
	c.onCreated = v
	return c
}

func (c WatchConsumer[E]) WithOnRestarted(v onWatcherRestarted) WatchConsumer[E] {
	c.onRestarted = v
	return c
}

func (c WatchConsumer[E]) WithOnError(v onWatcherError) WatchConsumer[E] {
	c.onError = v
	return c
}

func (c WatchConsumer[E]) WithOnClose(v onWatcherClose) WatchConsumer[E] {
	c.onClose = v
	return c
}

func (c WatchConsumer[E]) StartConsumer(wg *sync.WaitGroup) (initErr <-chan error) {
	initErrCh := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()

		// The flag restart=true is send with the first events batch after the "restarted" event, see WatchConsumer.forEachFn.
		restart := false

		// See watchErrorThreshold
		var lastErrorAt time.Time
		var lastError error

		// Channel is closed when the watcher context is cancelled,
		// so the context does not have to be checked here.
		for resp := range c.stream {
			switch {
			case resp.InitErr != nil:
				// Initialization error, the channel will be closed in the beginning of the next iteration.
				// Signal the problem via InitErr channel.
				// It is fatal error (e.g., no network connection), the app should be stopped and restarted.
				initErrCh <- resp.InitErr
				close(initErrCh)
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
					c.logger.Warn(resp.Err)
				} else {
					c.logger.Error(errors.Errorf(`%s (previous error %s ago)`, resp.Err, interval))
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
				c.logger.Warn(resp.RestartReason)
				if c.onRestarted != nil {
					c.onRestarted(resp.RestartReason, resp.RestartDelay)
				}
			case resp.Created:
				// The watcher has been successfully created.
				// This means transition from GetAll to Watch phase.
				if c.onCreated != nil {
					c.onCreated(resp.Header)
				}
				close(initErrCh)
			default:
				lastError = nil
				c.forEachFn(resp.Events, resp.Header, restart)
				restart = false
			}
		}

		if c.onClose != nil {
			if errors.Is(lastError, context.Canceled) || errors.Is(lastError, context.DeadlineExceeded) {
				// Context cancellation is the proper way to end the watch
				c.onClose(nil)
			} else {
				c.onClose(lastError)
			}
		}
	}()
	return initErrCh
}
