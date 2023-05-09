package store

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

func (s *Store) ListTokens(ctx context.Context, receiverKey key.ReceiverKey) (out []model.Token, err error) {
	ctx, span := s.telemetry.Tracer().Start(ctx, "keboola.go.buffer.store.ListTokens")
	defer telemetry.EndSpan(span, &err)

	tokens, err := s.getReceiverTokensOp(ctx, receiverKey).Do(ctx, s.client).All()
	if err != nil {
		return nil, err
	}

	return tokens.Values(), nil
}

func (s *Store) GetToken(ctx context.Context, exportKey key.ExportKey) (out model.Token, err error) {
	ctx, span := s.telemetry.Tracer().Start(ctx, "keboola.go.buffer.store.GetToken")
	defer telemetry.EndSpan(span, &err)

	resp, err := s.getTokenOp(ctx, exportKey).Do(ctx, s.client)
	if err != nil {
		return out, err
	}
	return resp.Value, err
}

func (s *Store) UpdateTokens(ctx context.Context, tokens []model.Token) (err error) {
	ctx, span := s.telemetry.Tracer().Start(ctx, "keboola.go.buffer.store.UpdateTokens")
	defer telemetry.EndSpan(span, &err)

	ops := make([]op.Op, 0, len(tokens))
	for _, token := range tokens {
		ops = append(ops, s.updateTokenOp(ctx, token))
	}

	_, err = op.MergeToTxn(ops...).Do(ctx, s.client)

	return err
}

func (s *Store) getReceiverTokensOp(_ context.Context, receiverKey key.ReceiverKey) iterator.DefinitionT[model.Token] {
	return s.schema.
		Secrets().
		Tokens().
		InReceiver(receiverKey).
		GetAll()
}

func (s *Store) updateTokenOp(_ context.Context, token model.Token) op.NoResultOp {
	return s.schema.
		Secrets().
		Tokens().
		InExport(token.ExportKey).
		Put(token)
}

func (s *Store) createTokenOp(_ context.Context, token model.Token) op.BoolOp {
	return s.schema.
		Secrets().
		Tokens().
		InExport(token.ExportKey).
		PutIfNotExists(token).
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, ok bool, err error) (bool, error) {
			if !ok && err == nil {
				return false, serviceError.NewResourceAlreadyExistsError("token", token.ID, "export")
			}
			return ok, err
		})
}

func (s *Store) getTokenOp(_ context.Context, exportKey key.ExportKey, opts ...etcd.OpOption) op.ForType[*op.KeyValueT[model.Token]] {
	return s.schema.
		Secrets().
		Tokens().
		InExport(exportKey).
		Get(opts...).
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[model.Token], err error) (*op.KeyValueT[model.Token], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewNoResourceFoundError("token", "export")
			}
			return kv, err
		})
}

func (s *Store) deleteExportTokenOp(_ context.Context, exportKey key.ExportKey) op.BoolOp {
	return s.schema.
		Secrets().
		Tokens().
		InExport(exportKey).
		Delete().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, result bool, err error) (bool, error) {
			if !result && err == nil {
				return false, serviceError.NewNoResourceFoundError("token", "export")
			}
			return result, err
		})
}

func (s *Store) deleteReceiverTokensOp(_ context.Context, receiverKey key.ReceiverKey) op.CountOp {
	return s.schema.
		Secrets().
		Tokens().
		InReceiver(receiverKey).
		DeleteAll()
}
