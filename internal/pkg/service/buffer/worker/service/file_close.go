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
	// ClosingFilesCheckInterval defines how often it will be checked
	// that each file in the "closing" state has a running "file.close" task.
	// This re-check mechanism provides retries for failed tasks or failed worker nodes.
	// In normal operation, switch to the "closing" state is processed immediately, on event from the Watch API.
	ClosingFilesCheckInterval = time.Minute

	fileCloseTaskType = "file.close"
)

// closeFiles watches for files switched to the closing state.
func (s *Service) closeFiles(slicesWatcher *activeSlicesWatcher, d dependencies) <-chan error {
	// Watch files in closing state
	return d.OrchestratorNode().Start(orchestrator.Config[model.File]{
		Name: fileCloseTaskType,
		Source: orchestrator.Source[model.File]{
			WatchPrefix:     s.schema.Files().Closing().PrefixT(),
			RestartInterval: ClosingFilesCheckInterval,
		},
		DistributionKey: func(event etcdop.WatchEventT[model.File]) string {
			file := event.Value
			return file.ReceiverKey.String()
		},
		TaskKey: func(event etcdop.WatchEventT[model.File]) task.Key {
			file := event.Value
			return task.Key{
				ProjectID: file.ProjectID,
				TaskID: task.ID(strings.Join([]string{
					file.ReceiverID.String(),
					file.ExportID.String(),
					file.FileID.String(),
					fileCloseTaskType,
				}, "/")),
			}
		},
		TaskCtx: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 2*time.Minute)
		},
		TaskFactory: func(event etcdop.WatchEventT[model.File]) task.Fn {
			return func(ctx context.Context, logger log.Logger) (result task.Result) {
				defer usererror.CheckAndWrap(&result.Error)

				// Wait until all slices are uploaded
				file := event.Value
				if err := slicesWatcher.WaitUntilAllSlicesUploaded(ctx, logger, file.FileKey); err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						// Log  a user error.
						// Timeout occurred while waiting, the error is elsewhere, not in this task.
						err = task.WrapUserError(err)
					}
					return task.ErrResult(err)
				}

				// Close the file, all slices have been closed.
				if err := s.store.CloseFile(ctx, &file); err != nil {
					return task.ErrResult(err)
				}

				return task.OkResult("file closed")
			}
		},
	})
}
