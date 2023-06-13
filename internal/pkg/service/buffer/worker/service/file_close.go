package service

import (
	"context"
	"strings"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task/orchestrator"
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
			return context.WithTimeout(context.Background(), time.Minute)
		},
		TaskFactory: func(event etcdop.WatchEventT[model.File]) task.Fn {
			return func(ctx context.Context, logger log.Logger) task.Result {
				// Wait until all slices are uploaded
				file := event.Value
				if err := slicesWatcher.WaitUntilAllSlicesUploaded(ctx, logger, file.FileKey); err != nil {
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
