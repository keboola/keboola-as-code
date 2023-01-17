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

func (u *Uploader) uploadSlices(ctx context.Context, wg *sync.WaitGroup, d dependencies) <-chan error {
	// Watch for slices switched to the uploading state.
	return orchestrator.Start(ctx, wg, d, orchestrator.Config[model.Slice]{
		Prefix:         u.schema.Slices().Uploading().PrefixT(),
		ReSyncInterval: 1 * time.Minute,
		TaskType:       "slice.upload",
		TaskFactory: func(event etcdop.WatchEventT[model.Slice]) task.Task {
			return func(_ context.Context, logger log.Logger) (string, error) {
				// Don't cancel upload on the shutdown
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer cancel()

				// Mark slice uploaded, update statistics
				slice := event.Value
				if err := u.store.MarkSliceUploaded(ctx, &slice); err != nil {
					return "", err
				}

				return "slice uploaded", nil
			}
		},
	})
}
