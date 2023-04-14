package service

import (
	"context"
	"sync"

	cleanupPkg "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/cleanup"
	cleanupTaskPkg "github.com/keboola/keboola-as-code/internal/pkg/service/common/task/cleanup"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (s *Service) cleanup(ctx context.Context, wg *sync.WaitGroup, d dependencies) <-chan error {
	logger := s.logger.AddPrefix("[cleanup]")
	node := cleanupPkg.NewNode(d, logger)

	initDone := make(chan error)
	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := s.clock.Ticker(s.config.CleanupInterval)
		defer ticker.Stop()

		logger.Infof("ready")
		close(initDone) // no error expected

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := node.Check(ctx); err != nil && !errors.Is(err, context.Canceled) {
					logger.Error(err)
				}
			}
		}
	}()

	return initDone
}

func (s *Service) cleanupTasks(ctx context.Context, wg *sync.WaitGroup, d dependencies) <-chan error {
	logger := s.logger.AddPrefix("[cleanup-tasks]")
	node := cleanupTaskPkg.NewNode(d, logger)

	initDone := make(chan error)
	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := s.clock.Ticker(s.config.CleanupInterval)
		defer ticker.Stop()

		logger.Infof("ready")
		close(initDone) // no error expected

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := node.Check(ctx); err != nil && !errors.Is(err, context.Canceled) {
					logger.Error(err)
				}
			}
		}
	}()

	return initDone
}
