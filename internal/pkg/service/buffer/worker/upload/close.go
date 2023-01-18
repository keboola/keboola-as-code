package upload

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

func (u *Uploader) closeSlices(ctx context.Context, wg *sync.WaitGroup, d dependencies) <-chan error {
	// Watch for slices switched to the closing state.
	return orchestrator.Start(ctx, wg, d, orchestrator.Config[model.Slice]{
		Prefix:         u.schema.Slices().Closing().PrefixT(),
		ReSyncInterval: time.Minute,
		TaskType:       "slice.close",
		TaskFactory: func(event etcdop.WatchEventT[model.Slice]) task.Task {
			return func(ctx context.Context, logger log.Logger) (string, error) {
				// On shutdown, the task is stopped immediately, because it is connected to the Uploader ctx.
				// There is no reason to wait, because it can be started again on another node.
				ctx, cancel := context.WithTimeout(ctx, time.Minute)
				defer cancel()

				// Wait until all API nodes switch to a new slice.
				rev := event.Kv.ModRevision
				logger.Infof(`waiting until all API nodes switch to a revision >= %v`, rev)
				if err := u.watcher.WaitForRevision(ctx, rev); err != nil {
					return "", err
				}

				// Close the slice, no API node is writing to it.
				slice := event.Value
				if err := u.store.CloseSlice(ctx, &slice); err != nil {
					return "", err
				}

				return "slice closed", nil
			}
		},
	})
}
