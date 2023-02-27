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

// FailedFilesCheckInterval defines how often it will be checked
// that File.RetryAfter time has expired, and the import task should be started again.
const FailedFilesCheckInterval = time.Minute

// retryFailedUploads watches for failed slices.
func (s *Service) retryFailedImports(ctx context.Context, wg *sync.WaitGroup, d dependencies) <-chan error {
	return orchestrator.Start(ctx, wg, d, orchestrator.Config[model.File]{
		Prefix:         s.schema.Files().Failed().PrefixT(),
		ReSyncInterval: FailedFilesCheckInterval,
		TaskType:       "file.retry.check",
		StartTaskIf: func(event etcdop.WatchEventT[model.File]) (string, bool) {
			file := event.Value
			now := model.UTCTime(s.clock.Now())
			needed := *file.RetryAfter
			if now.After(needed) {
				return "", true
			}
			return fmt.Sprintf(`File.RetryAfter condition not met, now: "%s", needed: "%s"`, now, needed), false
		},
		TaskFactory: func(event etcdop.WatchEventT[model.File]) task.Task {
			return func(_ context.Context, logger log.Logger) (result string, err error) {
				file := event.Value
				file.StorageJob = nil
				if err := s.store.ScheduleFileForRetry(ctx, &file); err != nil {
					return "", err
				}
				return "file scheduled for retry", nil
			}
		},
	})
}
