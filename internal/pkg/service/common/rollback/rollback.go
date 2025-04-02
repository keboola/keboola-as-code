// Package rollback provides a utility to handle rollback operations.
// - Start with the New function.
// - Then use Add method to add a rollback callback.
// - Or use AddLIFO or AddParallel methods to add a sub-container.
// - Finally call Invoke method if a rollback is needed.
package rollback

import (
	"context"
	"sync"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	StrategyLIFO     = strategy("lifo")
	StrategyParallel = strategy("parallel")
)

type Builder interface {
	Add(cb func(ctx context.Context) error)
	AddLIFO() *container
	AddParallel() *container
}

// Container is main LIFO container.
// Invoke method logs a warning.
type Container struct {
	logger log.Logger
	*container
}

// container is a sub-container with a defined strategy.
// Invoke method returns an error.
type container struct {
	strategy strategy

	lock      *sync.Mutex
	callbacks []callback
}

type strategy string

type callback func(ctx context.Context) error

// New creates top-level container with LIFO strategy.
// Errors are logged as a warning.
func New(logger log.Logger) *Container {
	return &Container{
		logger:    logger,
		container: newContainer(StrategyLIFO),
	}
}

func newContainer(strategy strategy) *container {
	return &container{
		strategy: strategy,
		lock:     &sync.Mutex{},
	}
}

func (v *Container) InvokeIfErr(ctx context.Context, errPtr *error) {
	if errPtr != nil && *errPtr != nil {
		v.Invoke(ctx)
	}
}

func (v *Container) Invoke(ctx context.Context) {
	ctx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), 30*time.Second, errors.New("rollback timeout"))
	defer cancel()
	if err := v.invokeOrErr(ctx); err != nil {
		v.logger.Warn(ctx, errors.PrefixError(err, "rollback failed").Error())
	}
}

// Add a rollback callback.
// Individual callbacks are invoked according to the LIFO or parallel strategy.
func (v *container) Add(cb func(ctx context.Context) error) {
	v.lock.Lock()
	v.callbacks = append(v.callbacks, cb)
	v.lock.Unlock()
}

func (v *container) AddLIFO() *container {
	container := newContainer(StrategyLIFO)
	v.Add(container.invokeOrErr)
	return container
}

func (v *container) AddParallel() *container {
	container := newContainer(StrategyParallel)
	v.Add(container.invokeOrErr)
	return container
}

func (v *container) invokeOrErr(ctx context.Context) error {
	switch v.strategy {
	case StrategyLIFO:
		return v.invokeLIFO(ctx)
	case StrategyParallel:
		return v.invokeParallel(ctx)
	default:
		panic(errors.Errorf(`unexpected strategy "%s"`, v.strategy))
	}
}

func (v *container) invokeLIFO(ctx context.Context) error {
	errs := errors.NewMultiError()

	// Iterate callbacks in LIFO order
	for i := len(v.callbacks) - 1; i >= 0; i-- {
		if err := v.callbacks[i](ctx); err != nil {
			errs.Append(err)
		}
	}

	return errs.ErrorOrNil()
}

func (v *container) invokeParallel(ctx context.Context) error {
	errs := errors.NewMultiError()

	// Invoke in parallel
	wg := &sync.WaitGroup{}
	for _, cb := range v.callbacks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := cb(ctx); err != nil {
				errs.Append(err)
			}
		}()
	}

	wg.Wait()
	return errs.ErrorOrNil()
}
