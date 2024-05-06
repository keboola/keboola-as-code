package slice

import (
	"context"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"time"
)

func (r *Repository) SwitchToUploading(k model.SliceKey, now time.Time) *op.AtomicOp[model.Slice] {
	return r.stateTransition(k, now, model.SliceClosing, model.SliceUploading)
}

func (r *Repository) SwitchToUploaded(k model.SliceKey, now time.Time) *op.AtomicOp[model.Slice] {
	return r.stateTransition(k, now, model.SliceUploading, model.SliceUploaded)
}

func (r *Repository) updateSlicesOnFileImport() {
	r.plugins.Collection().OnFileSave(func(ctx context.Context, now time.Time, original, updated *model.File) {
		if original != nil && original.State != updated.State && updated.State == model.FileImported {
			op.AtomicFromCtx(ctx).AddFrom(r.switchSlicesToImported(now, *updated))
		}
	})
}

func (r *Repository) switchSlicesToImported(now time.Time, file model.File) *op.AtomicOp[[]model.Slice] {
	var original, updated []model.Slice
	return op.Atomic(r.client, &updated).
		// Load slices
		ReadOp(r.ListIn(file.FileKey).WithAllTo(&original)).
		// Close slices
		WriteOrErr(func(ctx context.Context) (op.Op, error) {
			return r.switchStateInBatch(ctx, file.State, original, now, model.SliceUploaded, model.SliceImported)
		})
}

func (r *Repository) stateTransition(k model.SliceKey, now time.Time, from, to model.SliceState) *op.AtomicOp[model.Slice] {
	var file model.File
	var old, updated model.Slice
	return op.Atomic(r.client, &updated).
		// Read file
		ReadOp(r.files.Get(k.FileKey).WithResultTo(&file)).
		// Read entity for modification
		ReadOp(r.Get(k).WithResultTo(&old)).
		// Update the entity
		WriteOrErr(func(ctx context.Context) (op.Op, error) {
			txn, err := r.switchState(ctx, file.State, old, now, from, to)
			if err != nil {
				return nil, err
			}
			return txn.OnSucceeded(func(r *op.TxnResult[model.Slice]) {
				updated = r.Result()
			}), nil
		})
}

func (r *Repository) switchState(ctx context.Context, fileState model.FileState, oldValue model.Slice, now time.Time, from, to model.SliceState) (*op.TxnOp[model.Slice], error) {
	// Validate from state
	if oldValue.State != from {
		return nil, errors.Errorf(`slice "%s" is in "%s" state, expected "%s"`, oldValue.SliceKey, oldValue.State, from)
	}

	// Validate file and slice state combination
	if err := validateFileAndSliceState(fileState, to); err != nil {
		return nil, errors.PrefixErrorf(err, `unexpected slice "%s" state:`, oldValue.SliceKey)
	}

	// Switch slice state
	newValue, err := oldValue.WithState(now, to)
	if err != nil {
		return nil, err
	}

	return r.save(ctx, now, &oldValue, &newValue), nil
}

func (r *Repository) switchStateInBatch(ctx context.Context, fState model.FileState, original []model.Slice, now time.Time, from, to model.SliceState) (*op.TxnOp[[]model.Slice], error) {
	var updated []model.Slice
	txn := op.TxnWithResult(r.client, &updated)
	for _, slice := range original {
		if t, err := r.switchState(ctx, fState, slice, now, from, to); err == nil {
			txn.Merge(t.OnSucceeded(func(r *op.TxnResult[model.Slice]) {
				updated = append(updated, r.Result())
			}))
		} else {
			return nil, err
		}
	}
	return txn, nil
}

// ValidateFileAndSliceState validates combination of file and slice state.
func validateFileAndSliceState(fileState model.FileState, sliceState model.SliceState) error {
	switch fileState {
	case model.FileWriting, model.FileClosing:
		// Check allowed states
		switch sliceState {
		case model.SliceWriting, model.SliceClosing, model.SliceUploading, model.SliceUploaded:
			return nil
		default:
			// error
		}
	case model.FileImporting:
		// Slice must be uploaded
		if sliceState == model.SliceUploaded {
			return nil
		}
	case model.FileImported:
		// Slice must be marked as imported
		if sliceState == model.SliceImported {
			return nil
		}
	default:
		panic(errors.Errorf(`unexpected file state "%s`, fileState))
	}

	return serviceError.NewBadRequestError(
		errors.Errorf(`unexpected combination: file state "%s" and slice state "%s"`, fileState, sliceState),
	)
}
