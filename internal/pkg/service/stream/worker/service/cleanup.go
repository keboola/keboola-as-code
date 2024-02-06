package service

import (
	"context"

	cleanupPkg "github.com/keboola/keboola-as-code/internal/pkg/service/stream/cleanup"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (s *Service) cleanup(d dependencies) <-chan error {
	logger := s.logger.WithComponent("cleanup")
	node := cleanupPkg.NewNode(d, logger)

	initDone := make(chan error)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		ticker := s.clock.Ticker(s.config.TasksCleanupInterval)
		defer ticker.Stop()

		logger.Infof(s.ctx, "ready")
		close(initDone) // no error expected

		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				if err := node.Check(s.ctx); err != nil && !errors.Is(err, context.Canceled) {
					logger.Error(s.ctx, err.Error())
				}
			}
		}
	}()

	return initDone
}

func (s *Service) cleanupTasks() <-chan error {
	logger := s.logger.WithComponent("task.cleanup.ticker")
	initDone := make(chan error)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		ticker := s.clock.Ticker(s.config.TasksCleanupInterval)
		defer ticker.Stop()

		logger.Infof(s.ctx, "ready")
		close(initDone) // no error expected

		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				// Only one worker should do cleanup
				if s.dist.MustCheckIsOwner("task.cleanup") {
					if err := s.tasks.Cleanup(); err != nil && !errors.Is(err, context.Canceled) {
						logger.Error(s.ctx, err.Error())
					}
				}
			}
		}
	}()

	return initDone
}
