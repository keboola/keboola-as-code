package service

import (
	"context"
	"sync"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type cleanup struct {
	*Service
	logger   log.Logger
	assigner *distribution.Assigner
}

func (s *Service) cleanup(ctx context.Context, wg *sync.WaitGroup) <-chan error {
	return s.dist.StartWork(ctx, wg, s.logger, func(ctx context.Context, assigner *distribution.Assigner) <-chan error {
		c := &cleanup{
			Service:  s,
			logger:   s.logger.AddPrefix("[cleanup]"),
			assigner: assigner,
		}

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

		initDone := make(chan error)
		defer close(initDone)
		return initDone
	})
}

func (c *cleanup) check(ctx context.Context) {
	now := c.clock.Now()
	receivers, err := c.store.ListAllReceivers(ctx)
	if err != nil {
		c.logger.Error(err)
		return
	}

	for _, receiver := range receivers {
		if !c.assigner.MustCheckIsOwner(receiver.ReceiverKey.String()) {
			// Another worker node handles the resource.
			continue
		}

		err := c.store.Cleanup(ctx, receiver, c.logger)
		if err != nil && !errors.Is(err, context.Canceled) {
			c.logger.Error(err)
		}

		select {
		case <-ctx.Done():
			return
		case <-c.clock.After(100 * time.Millisecond):
			// let's wait a moment to not overload etcd database
		}
	}
	c.logger.Infof(`finished | %s`, c.clock.Since(now))
}
