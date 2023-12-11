// Package servicectx provides unique ID for a service process and support for the graceful shutdown.
package servicectx

import (
	"context"
	"fmt"
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
	uniqueID string

	logger   log.Logger
	wg       *sync.WaitGroup
	shutdown chan context.Context
	done     chan struct{}

	lock        *sync.Mutex
	terminating chan struct{}
	onShutdown  []OnShutdownFn
}

type ctxKey string

type OnShutdownFn func(ctx context.Context)

type ShutdownFn func(context.Context, error)

type Option func(c *config)

type config struct {
	uniqueID string
	logger   log.Logger
}

// WithUniqueID sets unique ID of the service process.
// By default, it is generated from the hostname and PID.
func WithUniqueID(v string) Option {
	return func(c *config) {
		c.uniqueID = v
	}
}

func WithLogger(v log.Logger) Option {
	return func(c *config) {
		c.logger = v
	}
}

func New(opts ...Option) (*Process, error) {
	// Apply options
	c := config{}
	for _, o := range opts {
		o(&c)
	}

	// Default logger
	if c.logger == nil {
		c.logger = log.NewNopLogger()
	}

	// Generate uniqueID if not set
	if c.uniqueID == "" {
		// Get hostname
		hostname, err := os.Hostname()
		if err != nil {
			return nil, err
		}

		// Get PID
		pid := os.Getpid()

		// Compose unique ID
		c.uniqueID = fmt.Sprintf(`%s-%05d`, hostname, pid)
	}

	v := &Process{
		uniqueID:    c.uniqueID,
		logger:      c.logger,
		wg:          &sync.WaitGroup{},
		done:        make(chan struct{}),
		lock:        &sync.Mutex{},
		terminating: make(chan struct{}),
		shutdown:    make(chan context.Context, 1),
	}

	// Execute OnShutdown callbacks and wait for them
	v.wg.Add(1)
	go func() {
		defer v.wg.Done()

		// Wait for shutdown, see Shutdown function
		shutdownCtx := <-v.shutdown

		// Iterate callbacks in reverse order, LIFO, see the OnShutdown method
		for i := len(v.onShutdown) - 1; i >= 0; i-- {
			v.onShutdown[i](shutdownCtx)
		}
	}()

	// Setup interrupt handler, so SIGINT and SIGTERM signals trigger shutdown.
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

		select {
		case sig := <-sigCh:
			// Trigger shutdown on signal
			v.Shutdown(context.Background(), errors.Errorf("%s", sig))
		case <-v.terminating:
			// The process was shut down by another trigger, stop goroutine
			return
		}
	}()

	// Finalizer
	go func() {
		// Wait for all work
		v.wg.Wait()

		// Log message after successful termination
		v.logger.Info("exited")

		// Unblock WaitForShutdown method calls
		close(v.done)
	}()

	v.logger.Infof(`process unique id "%s"`, v.UniqueID())
	return v, nil
}

func NewForTest(t *testing.T, opts ...Option) *Process {
	t.Helper()

	proc, err := New(opts...)
	if err != nil {
		t.Fatal(err)
		return nil
	}

	t.Cleanup(func() {
		proc.Shutdown(context.Background(), errors.New("test cleanup"))
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
		close(v.terminating)
		v.logger.Infof("exiting (%v)", err)
		v.shutdown <- ctx
		close(v.shutdown)
	}
}

// UniqueID returns unique process ID, it consists of hostname and PID.
func (v *Process) UniqueID() string {
	return v.uniqueID
}

// Add a root operation.
// The operation can terminate the entire process by calling the shutdown callback with an error.
func (v *Process) Add(operation func(ShutdownFn)) {
	v.lock.Lock()
	defer v.lock.Unlock()

	select {
	case <-v.terminating:
		v.logger.Errorf(`cannot Add operation: the Process is terminating`)
	default:
		v.wg.Add(1)
		go func() {
			defer v.wg.Done()
			operation(v.Shutdown)
		}()
	}
}

// OnShutdown registers a callback that is invoked when the process is terminating.
// Graceful shutdown waits until the callback has finished.
// Callbacks are invoked sequentially, in LIFO order.
func (v *Process) OnShutdown(fn OnShutdownFn) {
	v.lock.Lock()
	defer v.lock.Unlock()

	select {
	case <-v.terminating:
		v.logger.Errorf(`cannot register OnShutdown callback: the Process is terminating`)
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
