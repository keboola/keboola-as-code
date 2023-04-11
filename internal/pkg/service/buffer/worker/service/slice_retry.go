//nolint:dupl
package service

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/task/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	commonModel "github.com/keboola/keboola-as-code/internal/pkg/service/common/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	taskKey "github.com/keboola/keboola-as-code/internal/pkg/service/common/task/key"
)

const (
	// FailedSlicesCheckInterval defines how often it will be checked
	// that Slice.RetryAfter time has expired, and the upload task should be started again.
	FailedSlicesCheckInterval = time.Minute

	sliceRetryCheckTaskType = "slice.retry.check"
)

// retryFailedUploads watches for failed slices.
func (s *Service) retryFailedUploads(ctx context.Context, wg *sync.WaitGroup, d dependencies) <-chan error {
	return orchestrator.Start(ctx, wg, d, orchestrator.Config[model.Slice]{
		Name: sliceRetryCheckTaskType,
		Source: orchestrator.Source[model.Slice]{
			WatchPrefix:     s.schema.Slices().Failed().PrefixT(),
			RestartInterval: FailedSlicesCheckInterval,
		},
		DistributionKey: func(event etcdop.WatchEventT[model.Slice]) string {
			slice := event.Value
			return slice.ReceiverKey.String()
		},
		TaskKey: func(event etcdop.WatchEventT[model.Slice]) taskKey.Key {
			slice := event.Value
			return taskKey.Key{
				ProjectID: slice.ProjectID,
				TaskID: taskKey.ID(strings.Join([]string{
					slice.ReceiverID.String(),
					slice.ExportID.String(),
					slice.FileID.String(),
					slice.SliceID.String(),
					sliceRetryCheckTaskType,
				}, "/")),
			}
		},
		TaskCtx: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), time.Minute)
		},
		StartTaskIf: func(event etcdop.WatchEventT[model.Slice]) (string, bool) {
			slice := event.Value
			now := commonModel.UTCTime(s.clock.Now())
			needed := *slice.RetryAfter
			if now.After(needed) {
				return "", true
			}
			return fmt.Sprintf(`Slice.RetryAfter condition not met, now: "%s", needed: "%s"`, now, needed), false
		},
		TaskFactory: func(event etcdop.WatchEventT[model.Slice]) task.Task {
			return func(ctx context.Context, logger log.Logger) (result string, err error) {
				slice := event.Value
				if err := s.store.ScheduleSliceForRetry(ctx, &slice); err != nil {
					return "", err
				}
				return "slice scheduled for retry", nil
			}
		},
	})
}
