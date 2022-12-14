package store

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

func (s *Store) CreateFile(ctx context.Context, file model.File) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.CreateFile")
	defer telemetry.EndSpan(span, &err)

	_, err = s.createFileOp(ctx, file).Do(ctx, s.client)
	return err
}

func (s *Store) createFileOp(_ context.Context, file model.File) op.BoolOp {
	return s.schema.
		Files().
		Opened().
		ByKey(file.FileKey).
		PutIfNotExists(file).
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, ok bool, err error) (bool, error) {
			if !ok && err == nil {
				return false, serviceError.NewResourceAlreadyExistsError("file", file.FileKey.String(), "export")
			}
			return ok, err
		})
}

func (s *Store) GetFile(ctx context.Context, fileKey key.FileKey) (out model.File, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.GetFile")
	defer telemetry.EndSpan(span, &err)

	file, err := s.getFileOp(ctx, fileKey).Do(ctx, s.client)
	if err != nil {
		return out, err
	}
	return file.Value, nil
}

func (s *Store) getFileOp(_ context.Context, fileKey key.FileKey) op.ForType[*op.KeyValueT[model.File]] {
	return s.schema.
		Files().
		Opened().
		ByKey(fileKey).
		Get().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[model.File], err error) (*op.KeyValueT[model.File], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewResourceNotFoundError("file", fileKey.String())
			}
			return kv, err
		})
}
