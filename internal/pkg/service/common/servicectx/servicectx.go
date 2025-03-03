// Package servicectx provides unique ID for a service process and support for the graceful shutdown.
package servicectx

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// ctxShutdownReasonKey stores the error that triggered shutdown.
	ctxShutdownReasonKey = ctxKey("shutdownReason")
)

// Process is a stack of shutdown callbacks.
// Callbacks are invoked sequentially, in LIFO order.
// This makes it possible to register resource graceful shutdown callback when creating the resource.
// It is simple approach for complex applications.
//
// Callbacks are registered via the OnShutdown method.
//
// Root goroutines are registered via the Add method.
// Graceful shutdown waits for all root goroutines and shutdown callbacks.
//
// Graceful shutdown can be triggered by:
//   - Signals SIGINT, SIGTERM
//   - Call of the Shutdown method.
//   - Call of the ShutdownFn function provided by the Add method.
type Process struct {
	logger log.Logger
	// wg waits for all goroutines registered by the Add method.
	wg *sync.WaitGroup
	// done is closed when all OnShutdown callbacks and all goroutines, registered via Add, have been finished.
	done chan struct{}

	// lock synchronizes Add, OnShutdown and Shutdown methods, so these methods are atomic.
	lock *sync.Mutex
	// onShutdown is a list of shutdown callbacks invoked in LIFO order.
	onShutdown []OnShutdownFn
	// terminating is closed by the Shutdown method.
	terminating chan struct{}
	// shutdownCtx is set by the Shutdown method, before closing the "terminating" channel.
	shutdownCtx context.Context
}

type ctxKey string

type OnShutdownFn func(ctx context.Context)

type ShutdownFn func(context.Context, error)

type Option func(c *config)

type config struct {
	logger            log.Logger
	shutdownOnSignals bool
}

func WithLogger(v log.Logger) Option {
	return func(c *config) {
		c.logger = v
	}
}

func WithoutSignals() Option {
	return func(c *config) {
		c.shutdownOnSignals = false
	}
}

func New(opts ...Option) *Process {
	// Apply options
	c := config{shutdownOnSignals: true}
	for _, o := range opts {
		o(&c)
	}

	// Default logger
	if c.logger == nil {
		c.logger = log.NewNopLogger()
	}

	v := &Process{
		logger:      c.logger,
		wg:          &sync.WaitGroup{},
		done:        make(chan struct{}),
		lock:        &sync.Mutex{},
		terminating: make(chan struct{}),
	}

	// Execute OnShutdown callbacks and then, after all work, unblock WaitForShutdown via done channel
	go func() {
		// Wait for shutdown, see Shutdown function
		<-v.terminating

		// Iterate callbacks in reverse order, LIFO, see the OnShutdown method
		for i := len(v.onShutdown) - 1; i >= 0; i-- {
			v.onShutdown[i](v.shutdownCtx)
		}

		// Wait for all work
		v.wg.Wait()

		// Log message after successful termination
		v.logger.Info(v.shutdownCtx, "exited")

		// Unblock WaitForShutdown method calls
		close(v.done)
	}()

	// Setup interrupt handler, so SIGINT and SIGTERM signals trigger shutdown.
	if c.shutdownOnSignals {
		go func() {
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

			select {
			case sig := <-sigCh:
				// Trigger shutdown on signal
				v.Shutdown(context.Background(), errors.Errorf("%s", sig))
			case <-v.terminating:
				// The Process was shut down by another trigger, stop goroutine
				return
			}
		}()
	}

	return v
}

func NewForTest(tb testing.TB, opts ...Option) *Process {
	tb.Helper()

	proc := New(opts...)
	tb.Cleanup(func() {
		proc.Shutdown(tb.Context(), errors.New("test cleanup"))
		proc.WaitForShutdown()
	})

	return proc
}

// Shutdown triggers termination of the Process.
func (v *Process) Shutdown(ctx context.Context, err error) {
	ctx = context.WithValue(ctx, ctxShutdownReasonKey, err) // see ShutdownReason function

	v.lock.Lock()
	defer v.lock.Unlock()

	select {
	case <-v.terminating:
		return
	default:
		v.shutdownCtx = ctx
		v.logger.Infof(ctx, "exiting (%v)", err)
		close(v.terminating)
	}
}

// Add an operation.
// The Process is graceful terminated when all operations are completed.
// The ctx parameter can be used to wait for the service termination.
// The errCh parameter can be used to stop the service with an error.
func (v *Process) Add(operation func(ShutdownFn)) {
	v.wg.Add(1)
	go func() {
		defer v.wg.Done()
		operation(v.Shutdown)
	}()
}

// OnShutdown registers a callback that is invoked when the process is terminating.
// Graceful shutdown waits until the callback has finished.
// Callbacks are invoked sequentially, in LIFO order.
func (v *Process) OnShutdown(fn OnShutdownFn) {
	v.lock.Lock()
	defer v.lock.Unlock()

	select {
	case <-v.terminating:
		v.logger.Errorf(v.shutdownCtx, `cannot register OnShutdown callback: the Process is terminating`)
	default:
		v.onShutdown = append(v.onShutdown, fn)
	}
}

func (v *Process) WaitForShutdown() {
	<-v.done
}

// ShutdownReason gets the shutdown reason error.
func ShutdownReason(ctx context.Context) error {
	v, _ := ctx.Value(ctxShutdownReasonKey).(error)
	return v
}
