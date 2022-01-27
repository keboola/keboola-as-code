package local

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

const MaxLocalWorkers = 32

type Workers struct {
	ctx       context.Context
	started   *sync.WaitGroup
	semaphore *semaphore.Weighted
	group     *errgroup.Group
	workerNum *utils.SafeCounter
	lock      *sync.Mutex
	errors    map[int]error
	invoked   bool
}

func NewWorkers(parentCtx context.Context) *Workers {
	group, ctx := errgroup.WithContext(parentCtx)
	w := &Workers{
		ctx:       ctx,
		started:   &sync.WaitGroup{},
		semaphore: semaphore.NewWeighted(MaxLocalWorkers),
		workerNum: utils.NewSafeCounter(0),
		group:     group,
		lock:      &sync.Mutex{},
		errors:    make(map[int]error),
	}
	w.started.Add(1) // block all until Invoke called
	return w
}

func (w *Workers) AddWorker(worker func() error) {
	if w.invoked {
		panic(`invoked local.Workers cannot be reused`)
	}

	workerNumber := w.workerNum.GetAndInc()
	w.group.Go(func() error {
		w.started.Wait()

		// Limit maximum numbers of parallel filesystem operations.
		// It prevents problem with: too many open files
		if err := w.semaphore.Acquire(w.ctx, 1); err != nil {
			return err
		}
		defer w.semaphore.Release(1)

		if err := worker(); err != nil {
			w.lock.Lock()
			defer w.lock.Unlock()
			w.errors[workerNumber] = err
		}
		return nil
	})
}

func (w *Workers) StartAndWait() error {
	if w.invoked {
		panic(fmt.Errorf(`invoked local.Workers cannot be reused`))
	}

	// Unblock workers
	w.started.Done()

	// Wait for group
	errors := utils.NewMultiError()
	if err := w.group.Wait(); err != nil {
		errors.Append(err)
	}
	w.invoked = true

	// Collect errors in the same order as workers were defined
	workersCount := w.workerNum.Get()
	for i := 0; i < workersCount; i++ {
		if err, ok := w.errors[i]; ok {
			errors.Append(err)
		}
	}
	return errors.ErrorOrNil()
}
