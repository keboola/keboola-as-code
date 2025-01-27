package bridge

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-cloud-encrypt/pkg/cloudencrypt"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	keboolasink "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (b *Bridge) deleteTokenOnSinkDeactivation() {
	b.plugins.Collection().OnSinkDeactivation(func(ctx context.Context, now time.Time, by definition.By, original, sink *definition.Sink) error {
		if b.isKeboolaTableSink(sink) {
			api, err := b.apiProvider.APIFromContext(ctx)
			if err != nil {
				return err
			}

			op.AtomicOpCtxFrom(ctx).AddFrom(b.deleteToken(api, sink.SinkKey))
		}
		return nil
	})
}

func (b *Bridge) deleteToken(api *keboola.AuthorizedAPI, sinkKey key.SinkKey) *op.AtomicOp[op.NoResult] {
	var oldToken keboolasink.Token
	return op.Atomic(b.client, &op.NoResult{}).
		Read(func(ctx context.Context) op.Op {
			return b.schema.Token().ForSink(sinkKey).GetOrErr(b.client).WithResultTo(&oldToken)
		}).
		Write(func(ctx context.Context) op.Op {
			// Delete old token
			tokenID := oldToken.ID()
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

func (b *Bridge) tokenForSink(ctx context.Context, now time.Time, sink definition.Sink) (keboola.Token, error) {
	// Get token from database, if any.
	// The token is scoped to the sink bucket,
	// but an API operation can modify the target bucket,
	// then a new token must be generated.
	var existingToken *keboolasink.Token
	if !sink.CreatedAt().Time().Equal(now) {
		err := b.schema.Token().ForSink(sink.SinkKey).GetOrNil(b.client).WithResultTo(&existingToken).Do(ctx).Err()
		if err != nil {
			return keboola.Token{}, err
		}
	}

	// Prepare encryption metadata
	metadata := cloudencrypt.Metadata{"sink": sink.SinkKey.String()}

	// Use token from the database, if the operation is not called from the API,
	// so no modification of the sink target bucket is expected and the token should work.
	api, err := b.apiProvider.APIFromContext(ctx)
	if err != nil {
		if existingToken == nil {
			// Operation is not called from the API and there is no token in the database.
			return keboola.Token{}, serviceError.NewResourceNotFoundError("sink token", sink.SinkKey.String(), "database")
		}

		// Decrypt token
		token, err := existingToken.DecryptToken(ctx, b.tokenEncryptor, metadata)
		if err != nil {
			return keboola.Token{}, err
		}

		// Operation is not called from the API and there is a token in the database, so we are using the token.
		return token, nil
	}

	// Operation is called from the API (for example sink create/update), so we are creating a new token and deleting the old one,
	// if present, because the target bucket could have changed, and the old token would not work.
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
		return keboola.Token{}, err
	}

	// Register rollback
	rollback.FromContext(ctx).Add(func(ctx context.Context) error {
		b.logger.Info(ctx, "rollback: deleting token")
		return api.DeleteTokenRequest(result.ID).SendOrErr(ctx)
	})

	// Update atomic operation
	newToken = keboolasink.Token{
		SinkKey: sink.SinkKey,
		TokenID: result.ID,
	}

	if b.tokenEncryptor != nil {
		// Encrypt token
		ciphertext, err := b.tokenEncryptor.Encrypt(ctx, *result, metadata)
		if err != nil {
			return keboola.Token{}, err
		}
		newToken.EncryptedToken = string(ciphertext)
	} else {
		newToken.Token = result
	}

	op.AtomicOpCtxFrom(ctx).AddFrom(op.Atomic(b.client, &newToken).
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
			tokenID := existingToken.ID()
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
	return *result, nil
}

func (b *Bridge) MigrateTokens(ctx context.Context) error {
	if b.tokenEncryptor == nil {
		return nil
	}

	var tokens, updatedTokens []keboolasink.Token
	return op.Atomic(b.client, &updatedTokens).
		// Load tokens
		Read(func(ctx context.Context) op.Op {
			return b.schema.Token().GetAll(b.client).WithAllTo(&tokens)
		}).
		// Encrypt raw tokens
		Write(func(ctx context.Context) op.Op {
			return b.encryptRawTokens(ctx, tokens).SetResultTo(&updatedTokens)
		}).
		Do(ctx).Err()
}

func (b *Bridge) encryptRawTokens(ctx context.Context, tokens []keboolasink.Token) *op.TxnOp[[]keboolasink.Token] {
	var updated []keboolasink.Token
	txn := op.TxnWithResult(b.client, &updated)
	for _, token := range tokens {
		if token.Token == nil || token.EncryptedToken != "" {
			continue
		}

		txn.Merge(b.encryptToken(ctx, token).OnSucceeded(func(r *op.TxnResult[keboolasink.Token]) {
			updated = append(updated, r.Result())
		}))
	}
	return txn
}

func (b *Bridge) encryptToken(ctx context.Context, token keboolasink.Token) *op.TxnOp[keboolasink.Token] {
	metadata := cloudencrypt.Metadata{"sink": token.SinkKey.String()}
	ciphertext, err := b.tokenEncryptor.Encrypt(ctx, *token.Token, metadata)
	if err != nil {
		return op.ErrorTxn[keboolasink.Token](err)
	}
	token.TokenID = token.Token.ID
	token.EncryptedToken = string(ciphertext)

	return b.saveToken(ctx, token)
}

func (b *Bridge) saveToken(_ context.Context, token keboolasink.Token) *op.TxnOp[keboolasink.Token] {
	opKey := b.schema.Token().ForSink(token.SinkKey)

	saveTxn := op.TxnWithResult(b.client, &token)
	// Entity should exist
	saveTxn.Merge(op.Txn(b.client).
		If(etcd.Compare(etcd.ModRevision(opKey.Key()), "!=", 0)).
		OnFailed(func(r *op.TxnResult[op.NoResult]) {
			r.AddErr(serviceError.NewResourceNotFoundError("token", token.SinkKey.String(), "sink"))
		}),
	)

	saveTxn.Then(
		opKey.Put(b.client, token),
	)

	return saveTxn
}
