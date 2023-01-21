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
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// UploadingSlicesCheckInterval defines how often it will be checked
// that each slice in the "uploading" state has a running "slice.upload" task.
// This re-check mechanism provides retries for failed tasks or failed worker nodes.
// In normal operation, switch to the "uploading" state is processed immediately, we are notified via the Watch API.
const UploadingSlicesCheckInterval = time.Minute

// uploadSlices watches for slices switched to the uploading state.
func (u *Uploader) uploadSlices(ctx context.Context, wg *sync.WaitGroup, d dependencies) <-chan error {
	return orchestrator.Start(ctx, wg, d, orchestrator.Config[model.Slice]{
		Prefix:         u.schema.Slices().Uploading().PrefixT(),
		ReSyncInterval: UploadingSlicesCheckInterval,
		TaskType:       "slice.upload",
		TaskFactory: func(event etcdop.WatchEventT[model.Slice]) task.Task {
			return func(_ context.Context, logger log.Logger) (result string, err error) {
				// Don't cancel upload on the shutdown, but wait for timeout
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer cancel()

				// Get slice
				slice := event.Value

				// Handle error
				defer func() {
					if err != nil {
						attempt := slice.RetryAttempt + 1
						retryAfter := model.UTCTime(RetryAt(NewRetryBackoff(), u.clock.Now(), attempt))
						slice.RetryAttempt = attempt
						slice.RetryAfter = &retryAfter
						err = errors.Errorf(`slice upload failed: %w, upload will be retried after "%s"`, err, slice.RetryAfter)
						if err := u.store.MarkSliceUploadFailed(ctx, &slice); err != nil {
							u.logger.Errorf(`cannot mark the slice "%s" as failed: %s`, slice.SliceKey, err)
						}
					}
				}()

				// Skip empty
				if slice.IsEmpty {
					if err := u.store.MarkSliceUploaded(ctx, &slice); err != nil {
						return "", err
					}
					return "skipped upload of the empty slice", nil
				}

				// Load token
				token, err := u.store.GetToken(ctx, slice.ExportKey)
				if err != nil {
					return "", errors.Errorf(`cannot load token for export "%s": %w`, slice.ExportKey, err)
				}

				// Create file manager
				apiClient := storageapi.ClientWithHostAndToken(u.httpClient, u.storageAPIHost, token.Token)
				files := file.NewManager(d.Clock(), apiClient, u.config.uploadTransport)

				// Upload slice, set statistics
				reader := newRecordsReader(ctx, u.logger, u.etcdClient, u.schema, slice)
				if err := files.UploadSlice(ctx, &slice, reader); err != nil {
					return "", errors.Errorf(`file upload failed: %w`, err)
				}

				// Get all uploaded slices from the file
				var allSlices []model.Slice
				getSlicesOp := u.schema.Slices().Uploaded().InFile(slice.FileKey).
					GetAll().
					ForEachOp(func(s model.Slice, _ *iterator.Header) error {
						allSlices = append(allSlices, s)
						return nil
					})
				if err := getSlicesOp.DoOrErr(ctx, u.etcdClient); err != nil {
					return "", errors.Errorf(`get uploaded slices query failed: %w`, err)
				}

				// Update manifest, so the file is always importable.
				allSlices = append(allSlices, slice)
				if err := files.UploadManifest(ctx, slice.StorageResource, allSlices); err != nil {
					return "", errors.Errorf(`manifest upload failed: %w`, err)
				}

				// Mark slice uploaded
				if err := u.store.MarkSliceUploaded(ctx, &slice); err != nil {
					return "", err
				}

				return "slice uploaded", nil
			}
		},
	})
}
