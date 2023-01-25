package service

import (
	"context"
	"sync"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/task/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
)

// ClosingFilesCheckInterval defines how often it will be checked
// that each file in the "closing" state has a running "file.close" task.
// This re-check mechanism provides retries for failed tasks or failed worker nodes.
// In normal operation, switch to the "closing" state is processed immediately, on event from the Watch API.
const ClosingFilesCheckInterval = time.Minute

// closeFiles watches for files switched to the closing state.
func (s *Service) closeFiles(ctx context.Context, wg *sync.WaitGroup, d dependencies) <-chan error {
	// Watch un-uploaded slices
	w, initDone1 := NewActiveSlicesWatcher(ctx, wg, s.logger, s.schema, s.etcdClient)

	// Watch files in closing state
	initDone2 := orchestrator.Start(ctx, wg, d, orchestrator.Config[model.File]{
		Prefix:         s.schema.Files().Closing().PrefixT(),
		ReSyncInterval: ClosingFilesCheckInterval,
		TaskType:       "file.close",
		TaskFactory: func(event etcdop.WatchEventT[model.File]) task.Task {
			return func(ctx context.Context, logger log.Logger) (string, error) {
				// On shutdown, the task is stopped immediately, because it is connected to the Service ctx.
				// There is no reason to wait, because it can be started again on another node.
				ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
				defer cancel()

				// Wait until all slices are uploaded
				file := event.Value
				if err := w.WaitUntilAllSlicesUploaded(ctx, logger, file.FileKey); err != nil {
					return "", err
				}

				// Close the file, all slices have been closed.
				if err := s.store.CloseFile(ctx, &file); err != nil {
					return "", err
				}

				return "file closed", nil
			}
		},
	})

	// Wait for initialization of the both watchers
	initDone := make(chan error)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(initDone)
		if err := <-initDone1; err != nil {
			initDone <- err
		}
		if err := <-initDone2; err != nil {
			initDone <- err
		}
	}()

	return initDone
}
