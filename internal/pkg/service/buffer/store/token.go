package store

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
)

func (s *Store) createTokenOp(_ context.Context, token model.TokenForExport) op.BoolOp {
	return s.schema.
		Secrets().
		Tokens().
		InExport(token.ExportKey).
		PutIfNotExists(token).
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, ok bool, err error) (bool, error) {
			if !ok && err == nil {
				return false, serviceError.NewResourceAlreadyExistsError("token", token.ExportKey.String(), "export")
			}
			return ok, err
		})
}

func (s *Store) getTokenOp(_ context.Context, exportKey key.ExportKey) op.ForType[*op.KeyValueT[model.TokenForExport]] {
	return s.schema.
		Secrets().
		Tokens().
		InExport(exportKey).
		Get().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[model.TokenForExport], err error) (*op.KeyValueT[model.TokenForExport], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewResourceNotFoundError("token", exportKey.String())
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
				return false, serviceError.NewResourceNotFoundError("token", exportKey.String())
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
