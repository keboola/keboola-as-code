package distribution

import (
	"context"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// DistributedWork is a callback used by the Node.StartWork method.
//
// The responsibility of the callback is to start all tasks
// that are assigned to the worker node according to the current distribution.
// For this decision, the Assigner is provided as an argument.
//
// The context.Context is always cancelled, before the DistributedWork is started again.
//
// The DistributedWork callback is called:
//   - On initialization, after the Node.StartWork method is called.
//   - On distribution change.
//   - Periodically, according to the executorConfig.restartInterval.
type DistributedWork func(ctx context.Context, assigner *Assigner) (initDone <-chan error)

// StartWork starts the DistributedWork, see documentation there.
// Returned channel is used to indicate end of the initialization and may return an initialization error.
// In that case, the DistributedWork is cancelled.
func (n *Node) StartWork(ctx context.Context, wg *sync.WaitGroup, logger log.Logger, work DistributedWork, opts ...WorkOption) <-chan error {
	config := newWorkConfig(opts)
	initDone := make(chan error)

	wg.Add(1)
	go func() {
		defer wg.Done()

		distListener := n.OnChangeListener()
		defer distListener.Stop()

		restartTicker := n.clock.Ticker(config.restartInterval)
		defer restartTicker.Stop()

		initDone := initDone
		logger.Infof("ready")
		for {
			// Re-create work
			workCtx, cancelWork := context.WithCancel(ctx)
			if err := <-work(workCtx, n.CloneAssigner()); err != nil {
				if errors.Is(err, context.Canceled) {
					logger.Infof("work finished: %s", err)
				} else {
					logger.Errorf("work failed: %s", err)
				}
				if initDone != nil {
					initDone <- err
				}
			}

			// Close channel, after the first iteration
			if initDone != nil {
				close(initDone)
				initDone = nil
			}

			// Wait for stop or reset signal
			select {
			case <-ctx.Done():
				// Stop loop
				cancelWork()
				logger.Infof("stopped")
				return
			case events := <-distListener.C:
				// Reset on distribution change, because ownership of resources has been changed
				logger.Infof("restart: distribution changed: %s", events.Messages())
			case <-restartTicker.C:
				// Reset periodically to fix stuck states, e.g. some task failed, so it must be started again.
				logger.Debug("restart: periodical")
			}

			// Cancel previous work and try again
			cancelWork()
		}
	}()

	return initDone
}
