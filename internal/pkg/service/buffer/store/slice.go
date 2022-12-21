package store

import (
	"context"
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (s *Store) CreateSlice(ctx context.Context, slice model.Slice) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.CreateSlice")
	defer telemetry.EndSpan(span, &err)

	_, err = s.createSliceOp(ctx, slice).Do(ctx, s.client)
	return err
}

func (s *Store) createSliceOp(_ context.Context, slice model.Slice) op.BoolOp {
	return s.schema.
		Slices().
		Opened().
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

func (s *Store) GetLatestSlice(ctx context.Context, fileKey key.FileKey) (r model.Slice, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.GetLatestSlice")
	defer telemetry.EndSpan(span, &err)

	slice, err := s.getLatestSliceOp(ctx, fileKey).Do(ctx, s.client)
	if err != nil {
		return model.Slice{}, err
	}
	return slice.Value, nil
}

func (s *Store) getSliceOp(_ context.Context, sliceKey key.SliceKey) op.ForType[*op.KeyValueT[model.Slice]] {
	return s.schema.
		Slices().
		Opened().
		ByKey(sliceKey).
		Get().
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[model.Slice], err error) (*op.KeyValueT[model.Slice], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewResourceNotFoundError("slice", sliceKey.String())
			}
			return kv, err
		})
}

func (s *Store) getLatestSliceOp(_ context.Context, fileKey key.FileKey) op.ForType[*op.KeyValueT[model.Slice]] {
	return s.schema.
		Slices().
		Opened().
		InFile(fileKey).
		GetOne(etcd.WithSort(etcd.SortByKey, etcd.SortDescend)).
		WithProcessor(func(_ context.Context, _ etcd.OpResponse, kv *op.KeyValueT[model.Slice], err error) (*op.KeyValueT[model.Slice], error) {
			if kv == nil && err == nil {
				return nil, serviceError.NewResourceNotFoundError("latest slice", fileKey.String())
			}
			return kv, err
		})
}

func (s *Store) ListUploadedSlices(ctx context.Context, fileKey key.FileKey) (r []model.Slice, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.GetAllUploadedSlices")
	defer telemetry.EndSpan(span, &err)

	slices, err := s.listUploadedSlicesOp(ctx, fileKey).Do(ctx, s.client).All()
	if err != nil {
		return nil, err
	}

	return slices.Values(), nil
}

func (s *Store) listUploadedSlicesOp(_ context.Context, fileKey key.FileKey) iterator.DefinitionT[model.Slice] {
	return s.schema.
		Slices().
		Uploaded().
		InFile(fileKey).
		GetAll()
}

// SetSliceState method atomically changes the state of the file.
// False is returned, if the given file is already in the target state.
func (s *Store) SetSliceState(ctx context.Context, slice *model.Slice, to slicestate.State, now time.Time) (bool, error) { //nolint:dupl
	txn, err := s.setSliceStateOp(ctx, now, slice, to)
	if err != nil {
		return false, err
	}

	resp, err := txn.Do(ctx, s.client)
	if err != nil {
		return false, err
	}

	if err == nil && !resp.Succeeded {
		// File is already in the target state
		return false, nil
	}

	return true, nil
}

func (s *Store) setSliceStateOp(ctx context.Context, now time.Time, slice *model.Slice, to slicestate.State) (*op.TxnOp, error) { //nolint:dupl
	from := slice.State
	clone := *slice
	stm := slicestate.NewSTM(slice.State, func(ctx context.Context, from, to slicestate.State) error {
		// Update fields
		clone.State = to
		switch to {
		case slicestate.Closing:
			clone.ClosingAt = &now
		case slicestate.Closed:
			clone.ClosedAt = &now
		case slicestate.Uploading:
			clone.UploadingAt = &now
		case slicestate.Uploaded:
			clone.UploadedAt = &now
		case slicestate.Failed:
			clone.FailedAt = &now
		default:
			panic(errors.Errorf(`unexpected state "%s"`, to))
		}
		return nil
	})

	if err := stm.To(ctx, to); err != nil {
		return nil, err
	}

	// Atomically swap keys in the transaction
	txn := op.
		MergeToTxn(
			ctx,
			s.schema.Slices().InState(from).ByKey(slice.SliceKey).DeleteIfExists(),
			s.schema.Slices().InState(to).ByKey(slice.SliceKey).PutIfNotExists(clone),
		).
		WithProcessor(func(_ context.Context, _ *etcd.TxnResponse, result op.TxnResult, err error) error {
			if err == nil {
				*slice = clone
			}
			return err
		})

	return txn, nil
}
