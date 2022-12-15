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
	stm := slicestate.NewSTM(slice.State, func(ctx context.Context, from, to slicestate.State) error {
		// Update fields
		clone := *slice
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

		// Atomically swap keys in the transaction
		slices := s.schema.Slices()
		resp, err := op.MergeToTxn(
			ctx,
			slices.InState(from).ByKey(slice.SliceKey).DeleteIfExists(),
			slices.InState(to).ByKey(slice.SliceKey).PutIfNotExists(clone),
		).Do(ctx, s.client)

		// Check logical error, transaction failed
		if err == nil && !resp.Succeeded {
			slice.State = to
			return fileTxnFailedError{error: errors.Errorf(
				`transaction "%s" -> "%s" failed: the slice "%s" is already in the target state`,
				from, to, slice.SliceKey.String(),
			)}
		}

		if err == nil {
			*slice = clone
		}

		return err
	})

	if err := stm.To(ctx, to); err != nil {
		if errors.As(err, &fileTxnFailedError{}) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
