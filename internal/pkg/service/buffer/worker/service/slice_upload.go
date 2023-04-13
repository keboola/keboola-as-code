package service

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/file"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/task/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// UploadingSlicesCheckInterval defines how often it will be checked
	// that each slice in the "uploading" state has a running "slice.upload" task.
	// This re-check mechanism provides retries for failed tasks or failed worker nodes.
	// In normal operation, switch to the "uploading" state is processed immediately, we are notified via the Watch API.
	UploadingSlicesCheckInterval = time.Minute

	sliceUploadTaskType = "slice.upload"
)

// uploadSlices watches for slices switched to the uploading state.
func (s *Service) uploadSlices(ctx context.Context, wg *sync.WaitGroup, d dependencies) <-chan error {
	return orchestrator.Start(ctx, wg, d, orchestrator.Config[model.Slice]{
		Name: sliceUploadTaskType,
		Source: orchestrator.Source[model.Slice]{
			WatchPrefix:     s.schema.Slices().Uploading().PrefixT(),
			RestartInterval: UploadingSlicesCheckInterval,
		},
		DistributionKey: func(event etcdop.WatchEventT[model.Slice]) string {
			slice := event.Value
			return slice.ReceiverKey.String()
		},
		TaskKey: func(event etcdop.WatchEventT[model.Slice]) task.Key {
			slice := event.Value
			return task.Key{
				ProjectID: slice.ProjectID,
				TaskID: task.ID(strings.Join([]string{
					slice.ReceiverID.String(),
					slice.ExportID.String(),
					slice.FileID.String(),
					slice.SliceID.String(),
					sliceUploadTaskType,
				}, "/")),
			}
		},
		TaskCtx: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 5*time.Minute)
		},
		TaskFactory: func(event etcdop.WatchEventT[model.Slice]) task.Fn {
			return func(ctx context.Context, logger log.Logger) (result string, err error) {
				// Get slice
				slice := event.Value

				// Handle error
				defer func() {
					if err != nil {
						attempt := slice.RetryAttempt + 1
						retryAfter := utctime.UTCTime(RetryAt(NewRetryBackoff(), s.clock.Now(), attempt))
						slice.RetryAttempt = attempt
						slice.RetryAfter = &retryAfter
						err = errors.Errorf(`slice upload failed: %w, upload will be retried after "%s"`, err, slice.RetryAfter)
						if err := s.store.MarkSliceUploadFailed(ctx, &slice); err != nil {
							s.logger.Errorf(`cannot mark the slice "%s" as failed: %s`, slice.SliceKey, err)
						}
					}
				}()

				// Skip empty
				if slice.IsEmpty {
					if err := s.store.MarkSliceUploaded(ctx, &slice); err != nil {
						return "", err
					}
					return "skipped upload of the empty slice", nil
				}

				// Load token
				token, err := s.store.GetToken(ctx, slice.ExportKey)
				if err != nil {
					return "", errors.Errorf(`cannot load token for export "%s": %w`, slice.ExportKey, err)
				}

				api, err := keboola.NewAPI(ctx, s.storageAPIHost, keboola.WithClient(&s.httpClient), keboola.WithToken(token.Token))
				if err != nil {
					return "", err
				}

				defer s.events.SendSliceUploadEvent(ctx, api, time.Now(), &err, slice)

				// Create file manager
				files := file.NewManager(d.Clock(), api, s.config.UploadTransport)

				// Upload slice, set statistics
				reader := newRecordsReader(ctx, s.logger, s.etcdClient, s.schema, slice)
				if err := files.UploadSlice(ctx, &slice, reader); err != nil {
					return "", errors.Errorf(`file upload failed: %w`, err)
				}

				// Get all uploaded slices from the file
				var allSlices []model.Slice
				getSlicesOp := s.schema.Slices().Uploaded().InFile(slice.FileKey).
					GetAll().
					ForEachOp(func(s model.Slice, _ *iterator.Header) error {
						allSlices = append(allSlices, s)
						return nil
					})
				if err := getSlicesOp.DoOrErr(ctx, s.etcdClient); err != nil {
					return "", errors.Errorf(`get uploaded slices query failed: %w`, err)
				}

				// Update manifest, so the file is always importable.
				allSlices = append(allSlices, slice)
				if err := files.UploadManifest(ctx, slice.StorageResource, allSlices); err != nil {
					return "", errors.Errorf(`manifest upload failed: %w`, err)
				}

				// Mark slice uploaded
				if err := s.store.MarkSliceUploaded(ctx, &slice); err != nil {
					return "", err
				}

				return "slice uploaded", nil
			}
		},
	})
}
