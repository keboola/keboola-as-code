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

func (s *Store) createSliceOp(_ context.Context, slice model.Slice) op.BoolOp {
	return s.schema.
		Slices().
		ByKey(slice.SliceKey).
		PutIfNotExists(slice).
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, ok bool, err error) (bool, error) {
			if !ok && err == nil {
				return false, serviceError.NewResourceAlreadyExistsError("slice", slice.SliceKey.String(), "file")
			}
			return ok, err
		})
}

func (s *Store) GetSlice(ctx context.Context, sliceKey key.SliceKey) (r model.Slice, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.GetSlice")
	defer telemetry.EndSpan(span, &err)

	slice, err := s.getSliceOp(ctx, sliceKey).Do(ctx, s.client)
	if err != nil {
		return model.Slice{}, err
	}
	return slice.Value, nil
}

func (s *Store) getSliceOp(_ context.Context, sliceKey key.SliceKey) op.ForType[*op.KeyValueT[model.Slice]] {
	return s.schema.
		Slices().
		ByKey(sliceKey).
		Get().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[model.Slice], err error) (*op.KeyValueT[model.Slice], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewResourceNotFoundError("slice", sliceKey.String())
			}
			return kv, err
		})
}
