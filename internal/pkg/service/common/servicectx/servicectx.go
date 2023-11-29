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

type Process struct {
	ctx    context.Context
	cancel context.CancelFunc
	logger log.Logger
	wg     *sync.WaitGroup
	errCh  chan error

	lock        *sync.Mutex
	terminating bool
	onShutdown  []OnShutdownFn
}

type Option func(c *config)

type OnShutdownFn func()

type ShutdownFn func(error)

type config struct {
	logger log.Logger
}

func WithLogger(v log.Logger) Option {
	return func(c *config) {
		c.logger = v
	}
}

func New(ctx context.Context, cancel context.CancelFunc, opts ...Option) (*Process, error) {
	// Apply options
	c := config{}
	for _, o := range opts {
		o(&c)
	}

	// Default logger
	if c.logger == nil {
		c.logger = log.NewNopLogger()
	}

	// Create channel used by both the signal handler and service goroutines
	// to notify the main goroutine when to stop the server.
	errCh := make(chan error, 1)

	proc := &Process{
		ctx:    ctx,
		cancel: cancel,
		logger: c.logger,
		wg:     &sync.WaitGroup{},
		errCh:  errCh,
		lock:   &sync.Mutex{},
	}

	// Register onShutdown operation
	proc.Add(func(ctx context.Context, _ ShutdownFn) {
		<-ctx.Done()

		// Iterate callbacks in reverse order, LIFO, see the OnShutdown method
		for i := len(proc.onShutdown) - 1; i >= 0; i-- {
			proc.onShutdown[i]()
		}
	})

	// Setup interrupt handler,
	// so SIGINT and SIGTERM signals cause the services to stop gracefully.
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

		select {
		case <-ctx.Done():
			// Shutdown was triggered by another way
		case sig := <-sigCh:
			// Shutdown signal
			proc.Shutdown(errors.Errorf("%s", sig))
		}
	}()

	return proc, nil
}

func NewForTest(t *testing.T, ctx context.Context, opts ...Option) *Process {
	t.Helper()

	ctx, cancel := context.WithCancel(ctx)
	proc, err := New(ctx, cancel, opts...)
	if err != nil {
		t.Fatal(err)
		return nil
	}

	t.Cleanup(func() {
		proc.Shutdown(errors.New("test cleanup"))
		proc.WaitForShutdown()
	})

	return proc
}

// Ctx returns context of the Process.
// The context in canceled immediately as the process receives termination request.
// Then follows a graceful shutdown during which the context is already canceled.
func (v *Process) Ctx() context.Context {
	return v.ctx
}

// Shutdown triggers termination of the Process.
func (v *Process) Shutdown(err error) {
	v.lock.Lock()
	if v.terminating {
		v.lock.Unlock()
		return
	}
	v.terminating = true
	v.lock.Unlock()

	v.logger.Infof("exiting (%v)", err)
	v.errCh <- err
	close(v.errCh)
}

func (v *Process) WaitForShutdown() {
	// Wait for signal
	_, ok := <-v.errCh
	if !ok {
		// Channel is closed, the process is already terminating, wait for completion
		v.wg.Wait()
		return
	}

	// Send cancellation signal to the goroutines.
	v.cancel()

	// Wait for all operations
	v.wg.Wait()

	v.logger.Info("exited")
}

// Add an operation.
// The Process is graceful terminated when all operations are completed.
// The ctx parameter can be used to wait for the service termination.
// The errCh parameter can be used to stop the service with an error.
func (v *Process) Add(operation func(context.Context, ShutdownFn)) {
	v.wg.Add(1)
	go func() {
		defer v.wg.Done()
		operation(v.ctx, v.Shutdown)
	}()
}

// OnShutdown registers a callback that is invoked when the process is terminating.
// Graceful shutdown waits until the callback has finished.
// Callbacks are invoked sequentially, in LIFO order, see the New function.
func (v *Process) OnShutdown(fn OnShutdownFn) {
	v.lock.Lock()
	if v.terminating {
		v.logger.Errorf(`cannot register OnShutdown callback: the process is terminating`)
	}
	v.onShutdown = append(v.onShutdown, fn)
	v.lock.Unlock()
}
