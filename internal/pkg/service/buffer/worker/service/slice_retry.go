//nolint:dupl
package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/task/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
)

// FailedSlicesCheckInterval defines how often it will be checked
// that Slice.RetryAfter time has expired, and the upload task should be started again.
const FailedSlicesCheckInterval = time.Minute

// retryFailedUploads watches for failed slices.
func (s *Service) retryFailedUploads(ctx context.Context, wg *sync.WaitGroup, d dependencies) <-chan error {
	return orchestrator.Start(ctx, wg, d, orchestrator.Config[model.Slice]{
		Prefix:         s.schema.Slices().Failed().PrefixT(),
		ReSyncInterval: FailedSlicesCheckInterval,
		TaskType:       "slice.retry.check",
		StartTaskIf: func(event etcdop.WatchEventT[model.Slice]) (string, bool) {
			slice := event.Value
			now := model.UTCTime(s.clock.Now())
			needed := *slice.RetryAfter
			if now.After(needed) {
				return "", true
			}
			return fmt.Sprintf(`Slice.RetryAfter condition not met, now: "%s", needed: "%s"`, now, needed), false
		},
		TaskFactory: func(event etcdop.WatchEventT[model.Slice]) task.Task {
			return func(_ context.Context, logger log.Logger) (result string, err error) {
				slice := event.Value
				if err := s.store.ScheduleSliceForRetry(ctx, &slice); err != nil {
					return "", err
				}
				return "slice scheduled for retry", nil
			}
		},
	})
}
