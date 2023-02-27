package service

import (
	"context"
	"sync"
	"time"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/file"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/task/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// ImportingFilesCheckInterval defines how often it will be checked
// that each file in the "importing" state has a running "file.import" task.
// This re-check mechanism provides retries for failed tasks or failed worker nodes.
// In normal operation, switch to the "uploading" state is processed immediately, on event from the Watch API.
const ImportingFilesCheckInterval = time.Minute

// importFiles watches for files switched to the importing state.
func (s *Service) importFiles(ctx context.Context, wg *sync.WaitGroup, d dependencies) <-chan error {
	return orchestrator.Start(ctx, wg, d, orchestrator.Config[model.File]{
		Prefix:         s.schema.Files().Importing().PrefixT(),
		ReSyncInterval: ImportingFilesCheckInterval,
		TaskType:       "file.import",
		TaskFactory: func(event etcdop.WatchEventT[model.File]) task.Task {
			return func(_ context.Context, logger log.Logger) (result string, err error) {
				// Don't cancel import on the shutdown, but wait for timeout
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer cancel()

				// Get file
				fileRes := event.Value

				// Create event in the Storage API
				defer s.events.SendFileImportEvent(ctx, time.Now(), &err, fileRes)

				// Handle error
				defer func() {
					if err != nil {
						attempt := fileRes.RetryAttempt + 1
						retryAfter := model.UTCTime(RetryAt(NewRetryBackoff(), s.clock.Now(), attempt))
						fileRes.RetryAttempt = attempt
						fileRes.RetryAfter = &retryAfter
						err = errors.Errorf(`file import failed: %w, import will be retried after "%s"`, err, fileRes.RetryAfter)
						if err := s.store.MarkFileImportFailed(ctx, &fileRes); err != nil {
							s.logger.Errorf(`cannot mark the file "%s" as failed: %s`, fileRes.FileKey, err)
						}
					}
				}()

				// Skip empty
				if fileRes.IsEmpty {
					// Load token
					token, err := s.store.GetToken(ctx, fileRes.ExportKey)
					if err != nil {
						return "", errors.Errorf(`cannot load token for export "%s": %w`, fileRes.ExportKey, err)
					}

					// Create file manager
					api, err := keboola.NewAPI(ctx, s.storageAPIHost, keboola.WithClient(&s.httpClient), keboola.WithToken(token.Token))
					if err != nil {
						return "", err
					}
					files := file.NewManager(d.Clock(), api, s.config.uploadTransport)

					// Delete the empty file resource
					if err := files.DeleteFile(ctx, fileRes); err != nil {
						// The error is not critical
						s.logger.Error(errors.Errorf(`cannot delete empty file "%v/%v": %w`, fileRes.FileID, fileRes.StorageResource.ID, err))
					}

					// Mark file imported
					if err := s.store.MarkFileImported(ctx, &fileRes); err != nil {
						return "", err
					}
					return "skipped import of the empty file", nil
				}

				// Load token
				token, err := s.store.GetToken(ctx, fileRes.ExportKey)
				if err != nil {
					return "", errors.Errorf(`cannot load token for export "%s": %w`, fileRes.ExportKey, err)
				}

				// Create table manager
				api, err := keboola.NewAPI(ctx, s.storageAPIHost, keboola.WithClient(&s.httpClient), keboola.WithToken(token.Token))
				if err != nil {
					return "", err
				}
				tables := table.NewManager(api)

				// Import file
				job, err := tables.SendLoadDataRequest(ctx, fileRes)
				if err != nil {
					return "", err
				}

				// Store job
				fileRes.StorageJob = job
				if err := s.store.UpdateFile(ctx, fileRes); err != nil {
					return "", err
				}

				// Wait for job
				if err := tables.WaitForJob(ctx, job); err != nil {
					return "", err
				}

				// Mark file imported
				if err := s.store.MarkFileImported(ctx, &fileRes); err != nil {
					return "", err
				}

				return "file imported", nil
			}
		},
	})
}
