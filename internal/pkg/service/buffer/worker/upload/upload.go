package upload

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/file"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/task/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// UploadingSlicesCheckInterval defines how often it will be checked
// that each slice in the "uploading" state has a running "slice.upload" task.
// This re-check mechanism provides retries for failed tasks or failed worker nodes.
// In normal operation, switch to the "uploading" state is processed immediately, we are notified via the Watch API.
const UploadingSlicesCheckInterval = time.Minute

func (u *Uploader) uploadSlices(ctx context.Context, wg *sync.WaitGroup, d dependencies) <-chan error {
	// Watch for slices switched to the uploading state.
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

				// Load file
				fileRes, err := u.store.GetFile(ctx, slice.FileKey)
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
				files := file.NewManager(d.Clock(), apiClient, u.config.uploadTransport)

				// Upload slice, set statistics
				if err := files.UploadSlice(ctx, fileRes, &slice, u.newRecordsReader(ctx, slice)); err != nil {
					return "", errors.Errorf(`file upload failed: %w`, err)
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

func (u *Uploader) newRecordsReader(ctx context.Context, slice model.Slice) io.Reader {
	out, in := io.Pipe()
	go func() {
		var err error
		defer func() {
			if closeErr := in.CloseWithError(err); closeErr != nil {
				u.logger.Errorf(`cannot close records reader pipe: %w`, closeErr)
			}
		}()

		count := uint64(0)
		id := slice.IDRange.Start
		idPlaceholder := []byte(column.IDPlaceholder)
		if id < 1 {
			panic(errors.Errorf(`record ID must be > 0, found "%v"`, id))
		}

		// Read records
		records := u.schema.Records().InSlice(slice.SliceKey).GetAll().Do(ctx, u.etcdClient)
		for records.Next() {
			row := records.Value().Value
			row = bytes.ReplaceAll(row, idPlaceholder, []byte(strconv.FormatUint(id, 10)))
			_, err = in.Write(row)
			if err != nil {
				return
			}
			count++
			id++
		}

		// Check iterator error
		err = records.Err()
		if err != nil {
			return
		}

		// Check records count
		if count != slice.Statistics.RecordsCount {
			u.logger.Errorf(
				`unexpected number of uploaded records, expected "%d", found "%d"`,
				slice.Statistics.RecordsCount, count,
			)
		}
	}()
	return out
}
