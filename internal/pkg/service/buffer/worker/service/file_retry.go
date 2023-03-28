//nolint:dupl
package service

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/task/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
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
			WatchPrefix:    s.schema.Files().Failed().PrefixT(),
			WatchEvents:    []etcdop.EventType{etcdop.CreateEvent},
			ReSyncInterval: FailedFilesCheckInterval,
		},
		DistributionKey: func(event etcdop.WatchEventT[model.File]) string {
			file := event.Value
			return file.ReceiverKey.String()
		},
		StartTaskIf: func(event etcdop.WatchEventT[model.File]) (string, bool) {
			file := event.Value
			now := model.UTCTime(s.clock.Now())
			needed := *file.RetryAfter
			if !now.After(needed) {
				return fmt.Sprintf(`File.RetryAfter condition not met, now: "%s", needed: "%s"`, now, needed), false
			}

			return "", true
		},
		TaskKey: func(event etcdop.WatchEventT[model.File]) key.TaskKey {
			file := event.Value
			return key.TaskKey{
				ProjectID: file.ProjectID,
				TaskID: key.TaskID(strings.Join([]string{
					file.ReceiverID.String(),
					file.ExportID.String(),
					file.FileID.String(),
					fileRetryCheckTaskType,
				}, "/")),
			}
		},
		TaskCtx: func(ctx context.Context) (context.Context, context.CancelFunc) {
			// On shutdown, the task is not cancelled, but we wait for the timeout.
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
