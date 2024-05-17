package bridge

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/keboolasink"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (b *Bridge) GetToken(k key.SinkKey) op.WithResult[keboolasink.Token] {
	return b.schema.Token().ForSink(k).Get(b.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("sink token", k.String(), "database")
		})
}

func (b *Bridge) deleteTokenOnSinkDeactivation() {
	b.plugins.Collection().OnSinkDeactivation(func(ctx context.Context, now time.Time, by definition.By, original, sink *definition.Sink) error {
		if b.isKeboolaTableSink(sink) {
			api := b.apiProvider.MustAPIFromContext(ctx)
			op.AtomicOpFromCtx(ctx).AddFrom(b.deleteToken(api, sink.SinkKey))
		}
		return nil
	})
}

func (b *Bridge) deleteToken(api *keboola.AuthorizedAPI, sinkKey key.SinkKey) *op.AtomicOp[op.NoResult] {
	var oldToken *op.KeyValueT[keboolasink.Token]
	return op.Atomic(b.client, &op.NoResult{}).
		Read(func(ctx context.Context) op.Op {
			return b.schema.Token().ForSink(sinkKey).GetKV(b.client).WithResultTo(&oldToken)
		}).
		Write(func(ctx context.Context) op.Op {
			// Delete old token
			tokenID := oldToken.Value.Token.ID
			ctx = ctxattr.ContextWith(ctx, attribute.String("token.ID", tokenID))
			b.logger.Info(ctx, "deleting token")
			err := api.DeleteTokenRequest(tokenID).SendOrErr(ctx)
			// ErrorOp is not critical, log it only
			if err != nil {
				err = errors.Errorf(`cannot delete token: %w`, err)
				b.logger.Warn(ctx, err.Error())
			}

			b.logger.Info(ctx, "deleted token")

			// Delete token from database
			return b.schema.Token().ForSink(sinkKey).Delete(b.client)
		})
}

func (b *Bridge) tokenForSink(ctx context.Context, now time.Time, sink definition.Sink) (keboolasink.Token, error) {
	// Get token, is the sink is not new
	var existingToken *op.KeyValueT[keboolasink.Token]
	if !sink.CreatedAt().Time().Equal(now) {
		err := b.schema.Token().ForSink(sink.SinkKey).GetKV(b.client).WithResultTo(&existingToken).Do(ctx).Err()
		if err != nil {
			return keboolasink.Token{}, err
		}
	}

	// Use existing token, it the operation is not called from API
	api, apiFound := b.apiProvider.APIFromContext(ctx)
	if !apiFound {
		if existingToken == nil {
			return keboolasink.Token{}, serviceError.NewResourceNotFoundError("sink token", sink.SinkKey.String(), "database")
		} else {
			return existingToken.Value, nil
		}
	}

	name := fmt.Sprintf("[_internal] Stream Sink %s/%s", sink.SourceID, sink.SinkID)
	bucketID := sink.Table.Keboola.TableID.BucketID
	ctx = ctxattr.ContextWith(
		ctx,
		attribute.String("token.name", name),
		attribute.String("token.bucketID", bucketID.String()),
	)

	// Create new token based on the token from API authorization.
	var newToken keboolasink.Token
	b.logger.Info(ctx, "creating token")
	result, err := api.
		CreateTokenRequest(
			// Max length of description is 255 characters, this will be at most sourceID (48) + sinkID (48) + extra chars (24) = 120 characters.
			keboola.WithDescription(name),
			keboola.WithBucketPermissions(keboola.BucketPermissions{bucketID: keboola.BucketPermissionWrite}),
			keboola.WithCanReadAllFileUploads(true),
		).
		WithOnError(func(ctx context.Context, err error) error {
			// Improve error message, log warning
			err = errors.Errorf(`cannot create token: %w`, err)
			b.logger.Warn(ctx, err.Error())
			return err
		}).
		Send(ctx)
	if err != nil {
		return keboolasink.Token{}, err
	}

	// Register rollback
	rollback.FromContext(ctx).Add(func(ctx context.Context) error {
		b.logger.Info(ctx, "rollback: deleting token")
		return api.DeleteTokenRequest(result.ID).SendOrErr(ctx)
	})

	// Update atomic operation
	newToken = keboolasink.Token{SinkKey: sink.SinkKey, Token: *result}
	op.AtomicOpFromCtx(ctx).AddFrom(op.Atomic(b.client, &newToken).
		// Save token to database
		Write(func(ctx context.Context) op.Op {
			return b.schema.Token().ForSink(sink.SinkKey).Put(b.client, newToken)
		}).
		// Delete old token after the new token is saved
		AddProcessor(func(ctx context.Context, r *op.Result[keboolasink.Token]) {
			// Skip if the operation failed, or there is no old token
			if r.Err() != nil || existingToken == nil {
				return
			}

			// Delete old token
			tokenID := existingToken.Value.Token.ID
			ctx = ctxattr.ContextWith(ctx, attribute.String("token.ID", tokenID))
			b.logger.Info(ctx, "deleting old token")
			if err := api.DeleteTokenRequest(tokenID).SendOrErr(ctx); err != nil {
				// ErrorOp is not critical, log it only
				err = errors.Errorf(`cannot delete old token: %w`, err)
				b.logger.Warn(ctx, err.Error())
				return
			}

			b.logger.Info(ctx, "deleted old token")
		}),
	)

	b.logger.Info(ctx, "created token")
	return newToken, nil
}
