package service

import (
	"context"
	"sync"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/distribution"
)

type cleanup struct {
	*Service
	logger   log.Logger
	assigner *distribution.Assigner

	lock *sync.RWMutex
}

func (s *Service) cleanup(ctx context.Context, wg *sync.WaitGroup) <-chan error {
	return s.dist.StartWork(ctx, wg, s.logger, func(ctx context.Context, assigner *distribution.Assigner) (initDone <-chan error) {
		return startCleanup(ctx, wg, s, assigner)
	})
}

func startCleanup(ctx context.Context, wg *sync.WaitGroup, s *Service, assigner *distribution.Assigner) <-chan error {
	c := &cleanup{
		Service:  s,
		logger:   s.logger.AddPrefix("[cleanup]"),
		assigner: assigner,
		lock:     &sync.RWMutex{},
	}

	// Start ticker
	startTime := c.clock.Now()
	c.startTicker(ctx, wg)
	c.logger.Infof(`initialized | %s`, c.clock.Since(startTime))
	initDone := make(chan error, 1)
	defer close(initDone)
	return initDone
}

// startTicker to check conditions periodically.
func (c *cleanup) startTicker(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := c.clock.Ticker(c.config.cleanupInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.check(ctx)
			}
		}
	}()
}

func (c *cleanup) check(ctx context.Context) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	now := c.clock.Now()
	receivers, err := c.store.ListAllReceivers(ctx)
	if err != nil {
		c.logger.Error(err)
		return
	}

	for _, receiver := range receivers {
		select {
		case <-ctx.Done():
			return
		default:
		}

		err := c.store.Cleanup(ctx, receiver, c.logger)
		if err != nil {
			c.logger.Error(err)
		}

		time.Sleep(100 * time.Millisecond)
	}
	for _, receiver := range receivers {
		err := c.store.Cleanup(ctx, receiver, c.logger)
		if err != nil && !errors.Is(err, context.Canceled) {
			c.logger.Error(err)
		}

		select {
		case <-ctx.Done():
			return
                case <- c.clock.TimeAfter(100 * time.Millisecond):
                        // let's wait a while to not overload etcd database
		}
	}
	c.logger.Infof(`finished | %s`, c.clock.Since(now))
}
