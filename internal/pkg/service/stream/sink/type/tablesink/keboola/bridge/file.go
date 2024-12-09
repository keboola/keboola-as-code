package bridge

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-cloud-encrypt/pkg/cloudencrypt"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
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
				if err := b.ensureBucketExists(ctx, api, tableKey.BucketKey()); err != nil {
					return err
				}
			}

			// Get token from the database, or create a new one.
			token, err := b.tokenForSink(ctx, now, sink)
			if err != nil {
				return err
			}

			// Following API operations are always called using the limited token - scoped to the bucket.
			api := b.publicAPI.NewAuthorizedAPI(token.Token, 1*time.Minute)
			ctx = ctxattr.ContextWith(ctx, attribute.String("token.ID", token.ID))

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
			file.StagingStorage.Expiration = keboolaFile.Expiration()
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
		FileKey:               &file.FileKey,
		SinkKey:               file.SinkKey,
		TableID:               sink.Table.Keboola.TableID,
		Columns:               sink.Table.Mapping.Columns.Names(),
		FileID:                &stagingFile.FileID,
		FileName:              &stagingFile.Name,
		CredentialsExpiration: ptr.Ptr(utctime.From(stagingFile.CredentialsExpiration())),
	}
	if b.credentialsEncryptor != nil {
		// Encrypt credentials
		metadata := cloudencrypt.Metadata{"file": file.FileKey.String()}
		ciphertext, err := b.credentialsEncryptor.Encrypt(ctx, *stagingFile, metadata)
		if err != nil {
			return keboolasink.File{}, err
		}
		keboolaFile.EncryptedCredentials = ciphertext
	} else {
		keboolaFile.UploadCredentials = stagingFile
	}
	op.AtomicOpCtxFrom(ctx).Write(func(ctx context.Context) op.Op {
		return b.schema.File().ForFile(file.FileKey).Put(b.client, keboolaFile)
	})

	b.logger.Info(ctx, "created staging file")
	return keboolaFile, nil
}

// deleteCredentialsOnFileDelete deletes upload credentials from database, staging file is kept until its expiration.
func (b *Bridge) deleteCredentialsOnFileDelete() {
	b.plugins.Collection().OnFileDelete(func(ctx context.Context, now time.Time, original, file *model.File) error {
		if b.isKeboolaStagingFile(file) {
			op.AtomicOpCtxFrom(ctx).Write(func(ctx context.Context) op.Op {
				return b.schema.File().ForFile(file.FileKey).Delete(b.client)
			})
		}
		return nil
	})
}

func (b *Bridge) importFile(ctx context.Context, file plugin.File, stats statistics.Value) error {
	start := time.Now()

	// Get authorization token
	existingToken, err := b.schema.Token().ForSink(file.SinkKey).GetOrErr(b.client).Do(ctx).ResultOrErr()
	if err != nil {
		return err
	}

	// Get file details
	keboolaFile, err := b.schema.File().ForFile(file.FileKey).GetOrErr(b.client).Do(ctx).ResultOrErr()
	if err != nil {
		return err
	}

	// Compose keys
	tableKey := keboola.TableKey{BranchID: keboolaFile.SinkKey.BranchID, TableID: keboolaFile.TableID}
	fileKey := keboola.FileKey{BranchID: keboolaFile.SinkKey.BranchID, FileID: keboolaFile.ID()}

	// Add context attributes
	ctx = ctxattr.ContextWith(
		ctx,
		attribute.String("stagingFile.Name", keboolaFile.Name()),
		attribute.String("stagingFile.ID", fileKey.FileID.String()),
	)

	// Prepare encryption metadata
	metadata := cloudencrypt.Metadata{"sink": file.SinkKey.String()}

	// Decrypt token
	var token keboola.Token
	if existingToken.EncryptedToken != nil {
		token, err = b.tokenEncryptor.Decrypt(ctx, existingToken.EncryptedToken, metadata)
		if err != nil {
			return err
		}
	} else {
		token = *existingToken.Token
	}

	// Authorized API
	api := b.publicAPI.NewAuthorizedAPI(token.Token, 1*time.Minute)

	// Skip import if the file is empty.
	// The state is anyway switched to the FileImported by the operator.
	if file.IsEmpty {
		b.logger.Info(ctx, "empty file, skipped import, deleting empty staging file")
		if err := api.DeleteFileRequest(fileKey).SendOrErr(ctx); err != nil {
			b.logger.Warnf(ctx, "cannot delete empty staging file: %s", err.Error())
		}
		return nil
	}

	// Error when sending the event is not a fatal error
	defer func() {
		ctx, cancel := context.WithTimeout(ctx, b.config.EventSendTimeout)
		// We do not want to return err when send import event fails
		iErr := b.SendFileImportEvent(ctx, api, time.Since(start), &err, file.FileKey, stats)
		cancel()
		if iErr != nil {
			b.logger.Warnf(ctx, "unable to send file import event: %v", iErr)
			return
		}
	}()

	// Check if job already exists
	var job *keboola.StorageJob
	if keboolaFile.StorageJobID != nil {
		b.logger.With(attribute.String("job.id", keboolaFile.StorageJobID.String())).Infof(ctx, "storage job for file already exists")

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
		opts := []keboola.LoadDataOption{
			keboola.WithoutHeader(true),                     // the file is sliced, and without CSV header
			keboola.WithColumnsHeaders(keboolaFile.Columns), // fail, if the table columns differs
			keboola.WithIncrementalLoad(true),               // Append to file instead of overwriting
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

		// Save job ID to etcd
		err = b.createJob(ctx, file, job)
		if err != nil {
			return err
		}

		b.logger.Info(ctx, "created staging file")
	}

	// Wait for job to complete
	err = api.WaitForStorageJob(ctx, job)
	if err != nil {
		return err
	}

	return nil
}
