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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/keboolasink"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

const (
	fileNameDateFormat = "20060102150405"
)

// setupOnFileOpen creates staging file using Storage API and saves upload credentials to database.
func (b *Bridge) setupOnFileOpen() {
	b.plugins.Collection().OnFileOpen(func(ctx context.Context, now time.Time, sink definition.Sink, file *model.File) error {
		if b.isKeboolaTableSink(&sink) {
			// Create table if not exists
			tableKey := keboola.TableKey{BranchID: sink.BranchID, TableID: sink.Table.Keboola.TableID}
			if err := b.ensureTableExists(ctx, tableKey, sink); err != nil {
				return err
			}

			// Create file resource
			uploadCredentials, err := b.createStagingFile(ctx, now, sink, file)
			if err != nil {
				return err
			}

			// Update file entity
			file.Mapping = sink.Table.Mapping
			file.StagingStorage.Provider = stagingFileProvider // staging file is provided by the Keboola
			file.TargetStorage.Provider = targetProvider       // destination is a Keboola table
			file.StagingStorage.Expiration = utctime.From(uploadCredentials.CredentialsExpiration())
		}

		return nil
	})
}

func (b *Bridge) createStagingFile(ctx context.Context, now time.Time, sink definition.Sink, file *model.File) (keboolasink.File, error) {
	// Get token
	token, err := b.tokenForSink(ctx, now, sink)
	if err != nil {
		return keboolasink.File{}, err
	}

	// Get authorized Storage API
	api := b.publicAPI.WithToken(token.TokenString())

	name := fmt.Sprintf(`%s_%s_%s`, file.SourceID, file.SinkID, file.OpenedAt().Time().Format(fileNameDateFormat))
	ctx = ctxattr.ContextWith(
		ctx,
		attribute.String("token.ID", token.Token.ID),
		attribute.String("file.name", name),
		attribute.String("file.key", file.FileKey.String()),
	)

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
	entity := keboolasink.File{
		SinkKey: file.SinkKey,
		TableKey: keboola.TableKey{
			BranchID: sink.BranchID,
			TableID:  sink.Table.Keboola.TableID,
		},
		FileUploadCredentials: *stagingFile,
	}
	op.AtomicOpFromCtx(ctx).Write(func(ctx context.Context) op.Op {
		return b.schema.File().ForFile(file.FileKey).Put(b.client, entity)
	})

	b.logger.Info(ctx, "created staging file")
	return entity, nil
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

func (b *Bridge) registerFileImporter() {
	b.plugins.RegisterFileImporter(
		targetProvider,
		func(ctx context.Context, file *plugin.File) error {
			// Get authorization token
			var token *keboolasink.Token
			err := b.schema.Token().ForSink(file.SinkKey).GetOrNil(b.client).WithResultTo(&token).Do(ctx).Err()
			if err != nil {
				return err
			}

			// Get file upload credentials
			var keboolaFile *keboolasink.File
			err = b.schema.File().ForFile(file.FileKey).GetOrNil(b.client).WithResultTo(&keboolaFile).Do(ctx).Err()
			if err != nil {
				return err
			}

			// Authorized API
			api := b.publicAPI.WithToken(token.TokenString())

			// Create job to import data
			job, err := api.LoadDataFromFileRequest(keboolaFile.TableKey, keboolaFile.FileKey).Send(ctx)
			if err != nil {
				return err
			}

			// Wait for job to complete
			err = api.WaitForStorageJob(ctx, job)
			if err != nil {
				return err
			}

			return nil
		},
	)
}
