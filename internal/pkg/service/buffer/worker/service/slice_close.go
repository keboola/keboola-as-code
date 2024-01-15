package service

import (
	"context"
	"strings"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/usererror"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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
func (s *Service) closeSlices(d dependencies) <-chan error {
	return d.OrchestratorNode().Start(orchestrator.Config[model.Slice]{
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
			return context.WithTimeout(s.ctx, 2*time.Minute)
		},
		TaskFactory: func(event etcdop.WatchEventT[model.Slice]) task.Fn {
			return func(ctx context.Context, logger log.Logger) (result task.Result) {
				defer usererror.CheckAndWrap(&result.Error)

				// Wait until all API nodes switch to a new slice.
				waitCtx, waitCancel := context.WithTimeout(ctx, time.Minute)
				defer waitCancel()
				rev := event.Kv.CreateRevision
				logger.InfofCtx(ctx, `waiting until all API nodes switch to a revision >= %v`, rev)
				if err := s.watcher.WaitForRevision(waitCtx, rev); err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						// We did not receive confirmation from all API nodes
						// that they are no longer using the old slice,
						// there is some bug in the mechanism.
						logger.Error(ctx, errors.Errorf("a timeout occurred while waiting until all API nodes switch to a revision >= %v: %w", rev, err))
						// We will not block close and upload operation, because it would completely block the data flow.
						// Continue...
					} else {
						return task.ErrResult(err)
					}
				}

				// Close the slice, no API node is writing to it.
				slice := event.Value
				if err := s.store.CloseSlice(ctx, &slice); err != nil {
					return task.ErrResult(err)
				}

				return task.OkResult("slice closed")
			}
		},
	})
}
