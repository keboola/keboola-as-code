package upload

import (
	"context"
	"sync"
	"time"

	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/file"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/task/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (u *Uploader) uploadSlices(ctx context.Context, wg *sync.WaitGroup, d dependencies) <-chan error {
	// Watch for slices switched to the uploading state.
	return orchestrator.Start(ctx, wg, d, orchestrator.Config[model.Slice]{
		Prefix:         u.schema.Slices().Uploading().PrefixT(),
		ReSyncInterval: 1 * time.Minute,
		TaskType:       "slice.upload",
		TaskFactory: func(event etcdop.WatchEventT[model.Slice]) task.Task {
			return func(_ context.Context, logger log.Logger) (string, error) {
				// Don't cancel upload on the shutdown, but wait for timeout
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer cancel()

				// Get slice
				slice := event.Value

				// Skip empty
				if slice.IsEmpty {
					if err := u.store.MarkSliceUploaded(ctx, &slice); err != nil {
						return "", err
					}
					return "skipped upload of the empty slice", nil
				}

				// Load file
				_, err := u.store.GetFile(ctx, slice.FileKey)
				if err != nil {
					return "", errors.Errorf(`cannot load file "%s": %w`, slice.FileKey, err)
				}

				// Load token
				token, err := u.store.GetToken(ctx, slice.ExportKey)
				if err != nil {
					return "", errors.Errorf(`cannot load token for export "%s": %w`, slice.ExportKey, err)
				}

				// Create file manager
				apiClient := storageapi.ClientWithHostAndToken(u.httpClient, u.storageAPIHost, token.Token)
				_ = file.NewManager(d.Clock(), apiClient)

				// Upload slice
				// TODO

				// Mark slice uploaded, update statistics
				if err := u.store.MarkSliceUploaded(ctx, &slice); err != nil {
					return "", err
				}

				return "slice uploaded", nil
			}
		},
	})
}
