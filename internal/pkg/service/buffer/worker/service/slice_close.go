package service

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/task/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
)

const (
	// ClosingSlicesCheckInterval defines how often it will be checked
	// that each slice in the "closing" state has a running "slice.close" task.
	// This re-check mechanism provides retries for failed tasks or failed worker nodes.
	// In normal operation, switch to the "closing" state is processed immediately, we are notified via the Watch API.
	ClosingSlicesCheckInterval = time.Minute

	sliceCloseTaskType = "slice.close"
)

// closeSlices watches for slices switched to the closing state.
func (s *Service) closeSlices(ctx context.Context, wg *sync.WaitGroup, d dependencies) <-chan error {
	return orchestrator.Start(ctx, wg, d, orchestrator.Config[model.Slice]{
		Name: sliceCloseTaskType,
		Source: orchestrator.Source[model.Slice]{
			WatchPrefix:     s.schema.Slices().Closing().PrefixT(),
			RestartInterval: ClosingSlicesCheckInterval,
		},
		DistributionKey: func(event etcdop.WatchEventT[model.Slice]) string {
			slice := event.Value
			return slice.ReceiverKey.String()
		},
		TaskKey: func(event etcdop.WatchEventT[model.Slice]) task.Key {
			slice := event.Value
			return task.Key{
				ProjectID: slice.ProjectID,
				TaskID: task.ID(strings.Join([]string{
					slice.ReceiverID.String(),
					slice.ExportID.String(),
					slice.FileID.String(),
					slice.SliceID.String(),
					sliceCloseTaskType,
				}, "/")),
			}
		},
		TaskCtx: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(ctx, time.Minute)
		},
		TaskFactory: func(event etcdop.WatchEventT[model.Slice]) task.Fn {
			return func(ctx context.Context, logger log.Logger) (string, error) {
				// Wait until all API nodes switch to a new slice.
				rev := event.Kv.ModRevision
				logger.Infof(`waiting until all API nodes switch to a revision >= %v`, rev)
				if err := s.watcher.WaitForRevision(ctx, rev); err != nil {
					return "", err
				}

				// Close the slice, no API node is writing to it.
				slice := event.Value
				if err := s.store.CloseSlice(ctx, &slice); err != nil {
					return "", err
				}

				return "slice closed", nil
			}
		},
	})
}
