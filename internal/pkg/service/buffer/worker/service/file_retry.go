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
	// FailedFilesCheckInterval defines how often it will be checked
	// that File.RetryAfter time has expired, and the import task should be started again.
	FailedFilesCheckInterval = time.Minute

	fileRetryCheckTaskType = "file.retry.check"
)

// retryFailedUploads watches for failed slices.
func (s *Service) retryFailedImports(ctx context.Context, wg *sync.WaitGroup, d dependencies) <-chan error {
	return orchestrator.Start(ctx, wg, d, orchestrator.Config[model.File]{
		Name: fileRetryCheckTaskType,
		Source: orchestrator.Source[model.File]{
			WatchPrefix:     s.schema.Files().Failed().PrefixT(),
			RestartInterval: FailedFilesCheckInterval,
		},
		DistributionKey: func(event etcdop.WatchEventT[model.File]) string {
			file := event.Value
			return file.ReceiverKey.String()
		},
		StartTaskIf: func(event etcdop.WatchEventT[model.File]) (string, bool) {
			file := event.Value
			now := commonModel.UTCTime(s.clock.Now())
			needed := *file.RetryAfter
			if !now.After(needed) {
				return fmt.Sprintf(`File.RetryAfter condition not met, now: "%s", needed: "%s"`, now, needed), false
			}

			return "", true
		},
		TaskKey: func(event etcdop.WatchEventT[model.File]) taskKey.Key {
			file := event.Value
			return taskKey.Key{
				ProjectID: file.ProjectID,
				TaskID: taskKey.ID(strings.Join([]string{
					file.ReceiverID.String(),
					file.ExportID.String(),
					file.FileID.String(),
					fileRetryCheckTaskType,
				}, "/")),
			}
		},
		TaskCtx: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), time.Minute)
		},
		TaskFactory: func(event etcdop.WatchEventT[model.File]) task.Task {
			return func(ctx context.Context, logger log.Logger) (result string, err error) {
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
