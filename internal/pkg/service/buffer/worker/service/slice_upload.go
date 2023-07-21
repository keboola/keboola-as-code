package service

import (
	"context"
	"strings"
	"time"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/file"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// UploadingSlicesCheckInterval defines how often it will be checked
	// that each slice in the "uploading" state has a running "slice.upload" task.
	// This re-check mechanism provides retries for failed tasks or failed worker nodes.
	// In normal operation, switch to the "uploading" state is processed immediately, we are notified via the Watch API.
	UploadingSlicesCheckInterval = time.Minute
	sliceUploadTaskType          = "slice.upload"
	sliceMarkAsFailedTimeout     = 30 * time.Second
	uploadEventSendTimeout       = 10 * time.Second
)

// uploadSlices watches for slices switched to the uploading state.
func (s *Service) uploadSlices(d dependencies) <-chan error {
	return d.OrchestratorNode().Start(orchestrator.Config[model.Slice]{
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
			return func(ctx context.Context, logger log.Logger) (result task.Result) {
				// Get slice
				slice := event.Value

				// Handle error
				defer checkAndWrapUserError(&result.Error)
				defer func() {
					if result.IsError() {
						ctx, cancel := context.WithTimeout(context.Background(), fileMarkAsFailedTimeout)
						defer cancel()
						attempt := slice.RetryAttempt + 1
						retryAfter := utctime.UTCTime(RetryAt(NewRetryBackoff(), s.clock.Now(), attempt))
						slice.RetryAttempt = attempt
						slice.RetryAfter = &retryAfter
						result = result.WithError(errors.Errorf(`slice upload failed: %w, upload will be retried after "%s"`, result.Error, slice.RetryAfter))
						if err := s.store.MarkSliceUploadFailed(ctx, &slice); err != nil {
							s.logger.Errorf(`cannot mark the slice "%s" as failed: %s`, slice.SliceKey, err)
						}
					}
				}()

				// Skip empty
				if slice.IsEmpty {
					if err := s.store.MarkSliceUploaded(ctx, &slice, statistics.AfterUpload{}); err != nil {
						return task.ErrResult(err)
					}
					return task.OkResult("skipped upload of the empty slice")
				}

				// Load token
				token, err := s.store.GetToken(ctx, slice.ExportKey)
				if err != nil {
					return task.ErrResult(errors.Errorf(`cannot load token for export "%s": %w`, slice.ExportKey, err))
				}

				api, err := keboola.NewAPI(ctx, s.storageAPIHost, keboola.WithClient(&s.httpClient), keboola.WithToken(token.Token))
				if err != nil {
					return task.ErrResult(err)
				}

				// Generate Storage API event after the operation
				defer func() {
					ctx, cancel := context.WithTimeout(context.Background(), uploadEventSendTimeout)
					defer cancel()

					stats, statsErr := s.realtimeStats.SliceStats(ctx, slice.SliceKey)
					if statsErr != nil {
						s.logger.Errorf(`cannot send upload event: cannot get slice "%s" stats: %s`, slice.SliceKey, statsErr)
						return
					}

					s.events.SendSliceUploadEvent(ctx, api, time.Now(), &err, slice, stats.Uploaded)
				}()

				// Create file manager
				files := file.NewManager(d.Clock(), api, s.config.UploadTransport)

				// Get slice statistics
				stats, err := s.cachedStats.SliceStats(ctx, slice.SliceKey)
				if err != nil {
					return task.ErrResult(errors.Errorf(`cannot get slice "%s" stats: %w`, slice.SliceKey, err))
				}

				// Upload slice, set statistics
				uploadStats := statistics.AfterUpload{}
				reader := newRecordsReader(ctx, s.logger, s.etcdClient, s.schema, slice, stats.Total, &uploadStats)
				if err := files.UploadSlice(ctx, &slice, reader, &uploadStats); err != nil {
					return task.ErrResult(errors.Errorf(`file upload failed: %w`, err))
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
					return task.ErrResult(errors.Errorf(`get uploaded slices query failed: %w`, err))
				}

				// Update manifest, so the file is always importable.
				allSlices = append(allSlices, slice)
				if err := files.UploadManifest(ctx, slice.StorageResource, allSlices); err != nil {
					return task.ErrResult(errors.Errorf(`manifest upload failed: %w`, err))
				}

				// Mark slice uploaded
				if err := s.store.MarkSliceUploaded(ctx, &slice, uploadStats); err != nil {
					return task.ErrResult(err)
				}

				return task.OkResult("slice uploaded")
			}
		},
	})
}
