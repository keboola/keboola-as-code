package bridge

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	keboolasink "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

const (
	fileNameDateFormat = "20060102150405"
)

// setupOnFileOpen creates staging file using Storage API and saves upload credentials to database.
func (b *Bridge) setupOnFileOpen() {
	b.plugins.Collection().OnFileOpen(func(ctx context.Context, now time.Time, sink definition.Sink, file *model.File) error {
		if b.isKeboolaTableSink(&sink) {
			tableKey := keboola.TableKey{BranchID: sink.BranchID, TableID: sink.Table.Keboola.TableID}

			// Create bucket if not exists, but only if the operation is called via API.
			// We need a token with right permissions, to be able to create the bucket.
			// If the operation is not called via API, but from a background operator, the bucket should already exist.
			if api, err := b.apiProvider.APIFromContext(ctx); err == nil {
				if err := b.ensureBucketExistsBlocking(ctx, api, sink.ProjectID, tableKey); err != nil {
					return err
				}
			}

			// Get token from the database, or create a new one.
			token, err := b.tokenForSink(ctx, now, sink)
			if err != nil {
				return err
			}

			// Following API operations are always called using the limited token - scoped to the bucket.
			api := b.publicAPI.WithToken(token.TokenString())
			ctx = ctxattr.ContextWith(ctx, attribute.String("token.ID", token.Token.ID))

			// Create table if not exists
			if err := b.ensureTableExists(ctx, api, tableKey, sink); err != nil {
				return err
			}

			// Create file resource
			keboolaFile, err := b.createStagingFile(ctx, api, sink, file)
			if err != nil {
				return err
			}

			// Update file entity
			file.Mapping = sink.Table.Mapping
			file.StagingStorage.Provider = stagingFileProvider // staging file is provided by the Keboola
			file.TargetStorage.Provider = targetProvider       // destination is a Keboola table
			file.StagingStorage.Expiration = utctime.From(keboolaFile.UploadCredentials.CredentialsExpiration())
		}

		return nil
	})
}

func (b *Bridge) createStagingFile(ctx context.Context, api *keboola.AuthorizedAPI, sink definition.Sink, file *model.File) (keboolasink.File, error) {
	name := fmt.Sprintf(`%s_%s_%s`, file.SourceID, file.SinkID, file.OpenedAt().Time().Format(fileNameDateFormat))
	attributes := file.Telemetry()
	attributes = append(attributes, attribute.String("file.name", name))
	ctx = ctxattr.ContextWith(ctx, attributes...)

	// Create staging file
	b.logger.Info(ctx, `creating staging file`)
	stagingFile, err := api.CreateFileResourceRequest(
		file.BranchID,
		name,
		keboola.WithIsSliced(true),
		keboola.WithTags(
			fmt.Sprintf("stream.sourceID=%s", file.SourceID),
			fmt.Sprintf("stream.sinkID=%s", file.SinkID),
		),
	).Send(ctx)
	if err != nil {
		return keboolasink.File{}, err
	}

	// Register rollback
	rollback.FromContext(ctx).Add(func(ctx context.Context) error {
		b.logger.Info(ctx, "rollback: deleting staging file")
		return api.DeleteFileRequest(stagingFile.FileKey).SendOrErr(ctx)
	})

	// Save credentials to database
	ctx = ctxattr.ContextWith(ctx, attribute.String("file.resourceID", stagingFile.FileID.String()))
	keboolaFile := keboolasink.File{
		SinkKey:           file.SinkKey,
		TableID:           sink.Table.Keboola.TableID,
		Columns:           sink.Table.Mapping.Columns.Names(),
		UploadCredentials: *stagingFile,
	}
	op.AtomicOpFromCtx(ctx).Write(func(ctx context.Context) op.Op {
		return b.schema.File().ForFile(file.FileKey).Put(b.client, keboolaFile)
	})

	b.logger.Info(ctx, "created staging file")
	return keboolaFile, nil
}

// deleteCredentialsOnFileDelete deletes upload credentials from database, staging file is kept until its expiration.
func (b *Bridge) deleteCredentialsOnFileDelete() {
	b.plugins.Collection().OnFileDelete(func(ctx context.Context, now time.Time, original, file *model.File) error {
		if b.isKeboolaStagingFile(file) {
			op.AtomicOpFromCtx(ctx).Write(func(ctx context.Context) op.Op {
				return b.schema.File().ForFile(file.FileKey).Delete(b.client)
			})
		}
		return nil
	})
}

func (b *Bridge) importFile(ctx context.Context, file *plugin.File, stats statistics.Value) error {
	start := time.Now()

	// Get authorization token
	token, err := b.schema.Token().ForSink(file.SinkKey).GetOrErr(b.client).Do(ctx).ResultOrErr()
	if err != nil {
		return err
	}

	// Get file details
	keboolaFile, err := b.schema.File().ForFile(file.FileKey).GetOrErr(b.client).Do(ctx).ResultOrErr()
	if err != nil {
		return err
	}

	// Authorized API
	api := b.publicAPI.WithToken(token.TokenString())

	// Error when sending the event is not a fatal error
	defer func() {
		ctx, cancel := context.WithTimeout(ctx, b.config.EventSendTimeout)
		err = b.SendFileImportEvent(ctx, api, time.Since(start), &err, file.FileKey, stats)
		cancel()
		if err != nil {
			b.logger.Errorf(ctx, "unable to send file import event: %v", err)
			return
		}
	}()

	// Check if job already exists
	var job *keboola.StorageJob
	if keboolaFile.StorageJobID != nil {
		job, err = api.GetStorageJobRequest(keboola.StorageJobKey{ID: *keboolaFile.StorageJobID}).Send(ctx)
		if err != nil {
			return err
		}

		if job.Status == keboola.StorageJobStatusSuccess {
			return nil
		}
	}

	// Create job to import data if no job exists yet or if it failed
	if job == nil || job.Status == keboola.StorageJobStatusError {
		tableKey := keboola.TableKey{BranchID: keboolaFile.SinkKey.BranchID, TableID: keboolaFile.TableID}
		fileKey := keboola.FileKey{BranchID: keboolaFile.SinkKey.BranchID, FileID: keboolaFile.UploadCredentials.FileID}
		opts := []keboola.LoadDataOption{
			keboola.WithoutHeader(true),                     // the file is sliced, and without CSV header
			keboola.WithColumnsHeaders(keboolaFile.Columns), // fail, if the table columns differs
			keboola.WithIncrementalLoad(true),               // Append to file instead of overwritting
		}
		job, err = api.LoadDataFromFileRequest(tableKey, fileKey, opts...).Send(ctx)
		if err != nil {
			return err
		}

		// Save job ID to etcd
		keboolaFile.StorageJobID = &job.ID
		err = b.schema.File().ForFile(file.FileKey).Put(b.client, keboolaFile).Do(ctx).Err()
		if err != nil {
			return err
		}
	}

	// Wait for job to complete
	err = api.WaitForStorageJob(ctx, job)
	if err != nil {
		return err
	}

	return nil
}
