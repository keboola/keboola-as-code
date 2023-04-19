package service

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	filePkg "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/file"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/task/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// ImportingFilesCheckInterval defines how often it will be checked
	// that each file in the "importing" state has a running "file.import" task.
	// This re-check mechanism provides retries for failed tasks or failed worker nodes.
	// In normal operation, switch to the "uploading" state is processed immediately, on event from the Watch API.
	ImportingFilesCheckInterval = time.Minute

	fileImportTaskType = "file.import"
)

// importFiles watches for files switched to the importing state.
func (s *Service) importFiles(ctx context.Context, wg *sync.WaitGroup, d dependencies) <-chan error {
	return orchestrator.Start(ctx, wg, d, orchestrator.Config[model.File]{
		Name: fileImportTaskType,
		Source: orchestrator.Source[model.File]{
			WatchPrefix:     s.schema.Files().Importing().PrefixT(),
			RestartInterval: ImportingFilesCheckInterval,
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
					fileImportTaskType,
				}, "/")),
			}
		},
		TaskCtx: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 5*time.Minute)
		},
		TaskFactory: func(event etcdop.WatchEventT[model.File]) task.Fn {
			return func(ctx context.Context, logger log.Logger) (result task.Result) {
				// Get file
				fileRes := event.Value

				// Handle error
				defer func() {
					if result.IsErr() {
						attempt := fileRes.RetryAttempt + 1
						retryAfter := utctime.UTCTime(RetryAt(NewRetryBackoff(), s.clock.Now(), attempt))
						fileRes.RetryAttempt = attempt
						fileRes.RetryAfter = &retryAfter
						result = result.WithErr(errors.Errorf(`file import failed: %w, import will be retried after "%s"`, result.Err(), fileRes.RetryAfter))
						if err := s.store.MarkFileImportFailed(ctx, &fileRes); err != nil {
							s.logger.Errorf(`cannot mark the file "%s" as failed: %s`, fileRes.FileKey, err)
						}
					}
				}()

				// Load token
				token, err := s.store.GetToken(ctx, fileRes.ExportKey)
				if err != nil {
					return task.ErrResult(errors.Errorf(`cannot load token for export "%s": %w`, fileRes.ExportKey, err))
				}

				api, err := keboola.NewAPI(ctx, s.storageAPIHost, keboola.WithClient(&s.httpClient), keboola.WithToken(token.Token))
				if err != nil {
					return task.ErrResult(err)
				}

				defer s.events.SendFileImportEvent(ctx, api, time.Now(), &err, fileRes)

				// Skip empty
				if fileRes.IsEmpty {
					// Create file manager
					api, err := keboola.NewAPI(ctx, s.storageAPIHost, keboola.WithClient(&s.httpClient), keboola.WithToken(token.Token))
					if err != nil {
						return task.ErrResult(err)
					}
					files := filePkg.NewManager(d.Clock(), api, s.config.UploadTransport)

					// Delete the empty file resource
					if err := files.DeleteFile(ctx, fileRes); err != nil {
						// The error is not critical
						s.logger.Error(errors.Errorf(`cannot delete empty file "%v/%v": %s`, fileRes.FileID, fileRes.StorageResource.ID, err))
					}

					// Mark file imported
					if err := s.store.MarkFileImported(ctx, &fileRes); err != nil {
						return task.ErrResult(err)
					}
					return task.OkResult("skipped import of the empty file")
				} else {
					// Create table manager
					tables := table.NewManager(api)

					// StorageJob may exist if the previous worker unexpectedly failed
					if fileRes.StorageJob == nil {
						// Import file
						job, err := tables.SendLoadDataRequest(ctx, fileRes)
						if err != nil {
							return task.ErrResult(err)
						}

						// Store job
						fileRes.StorageJob = job
						if err := s.store.UpdateFile(ctx, fileRes); err != nil {
							return task.ErrResult(err)
						}
					}

					// Wait for job
					if err := tables.WaitForJob(ctx, fileRes.StorageJob); err != nil {
						return task.ErrResult(err)
					}

					// Mark file imported
					if err := s.store.MarkFileImported(ctx, &fileRes); err != nil {
						return task.ErrResult(err)
					}

					return task.OkResult("file imported")
				}
			}
		},
	})
}
