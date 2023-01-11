package distribution

import (
	"context"
	"fmt"
	"sync"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

// Executor restarts the ExecutorWork every time the distribution changes.
// See documentation of the ExecutorWork and Node.StartExecutor method.
type Executor struct {
	clock        clock.Clock
	logger       log.Logger
	distribution *Node
	work         ExecutorWork
	resetCh      <-chan reason
}

// ExecutorWork is a callback wrapped by the Executor.
// It is used by the Node.StartExecutor method.
//
// The responsibility of the callback is to start all tasks
// that belong to the worker node according to the current distribution.
// For this decision, the Assigner is provided as an argument.
//
// The old context.Context is always cancelled, before the ExecutorWork is called again.
//
// During shutdown, the Executor waits via the sync.WaitGroup until all ExecutorWork callbacks are finished.
//
// The ExecutorWork callback is called:
//   - On executor creation/initialization.
//   - On distribution change.
//   - Periodically, according to the executorConfig.ResetInterval, to handle stuck states,
//     e.g. some worker gone and/or the operation failed, so it must be started again.
type ExecutorWork func(ctx context.Context, wg *sync.WaitGroup, logger log.Logger, assigner *Assigner) (initErr error)

// reason of the ExecutorWork reset.
type reason string

func startExecutor(n *Node, name string, work ExecutorWork, opts ...ExecutorOption) error {
	// Apply options
	c := defaultExecutorConfig()
	for _, o := range opts {
		o(&c)
	}

	// Create
	resetCh := make(chan reason, 1)
	v := &Executor{
		clock:        n.clock,
		logger:       n.logger.AddPrefix(fmt.Sprintf("[%s]", name)),
		distribution: n,
		work:         work,
		resetCh:      resetCh,
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	n.proc.OnShutdown(func() {
		v.logger.Info("received shutdown request")
		cancel()
		wg.Wait()
		v.logger.Info("shutdown done")
	})

	// Reset channel triggers context cancellation and restart of the ExecutorWork, see startLoop method.
	wg.Add(1)
	go func() {
		defer wg.Done()

		distListener := v.distribution.OnChangeListener()
		defer distListener.Stop()

		resetTicker := v.clock.Ticker(c.resetInterval)
		defer resetTicker.Stop()

		resetCh <- "initialization"

		for {
			select {
			case <-ctx.Done():
				// Stop loop
				close(resetCh)
				return
			case events := <-distListener.C:
				// Reset ExecutorWork on distribution change, because ownership of resources has been changed
				resetCh <- reason(fmt.Sprintf(`distribution changed: %s`, events.Messages()))
			case <-resetTicker.C:
				// Reset ExecutorWork periodically to fix stuck states, e.g. some worker gone or the operation failed, so it must be started again.
				resetCh <- "periodical"
			}
		}
	}()

	// Start loop, return the initialization error
	return <-v.startLoop(ctx, wg)
}

func (v *Executor) startLoop(mainCtx context.Context, wg *sync.WaitGroup) <-chan error {
	var ctx context.Context
	var cancel context.CancelFunc

	init := true
	initDone := make(chan error)
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Channel is closed on shutdown, so the goroutine will stop
		for resetReason := range v.resetCh {
			// Cancel previous work, if any
			if cancel != nil {
				cancel()
			}

			// Log reset reason
			v.logger.Infof("reset: %s", resetReason)

			// Re-create work
			ctx, cancel = context.WithCancel(mainCtx)
			workInitErr := v.work(ctx, wg, v.logger, v.distribution.CloneAssigner())

			// Cancel work on the initialization error
			if workInitErr != nil {
				cancel()
				cancel = nil
			}

			// Pass the work initialization error on executor initialization/first run
			if init {
				if workInitErr != nil {
					initDone <- workInitErr
				}
				close(initDone)
			}

			// Log the work initialization error and try again on the next reset
			if workInitErr != nil {
				v.logger.Errorf("initialization failed: %s", workInitErr)
			}

			init = false
		}
	}()

	return initDone
}
