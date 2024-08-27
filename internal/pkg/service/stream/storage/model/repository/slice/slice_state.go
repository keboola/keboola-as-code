package slice

import (
	"context"
	"time"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type switchStateOption func(file *model.Slice)

func withIsEmpty(isEmpty bool) switchStateOption {
	return func(file *model.Slice) {
		file.LocalStorage.IsEmpty = isEmpty
	}
}

func (r *Repository) SwitchToUploading(k model.SliceKey, now time.Time, isEmpty bool) *op.AtomicOp[model.Slice] {
	return r.stateTransition(k, now, model.SliceClosing, model.SliceUploading, withIsEmpty(isEmpty))
}

func (r *Repository) SwitchToUploaded(k model.SliceKey, now time.Time) *op.AtomicOp[model.Slice] {
	return r.stateTransition(k, now, model.SliceUploading, model.SliceUploaded)
}

func (r *Repository) updateSlicesOnFileImport() {
	r.plugins.Collection().OnFileSave(func(ctx context.Context, now time.Time, original, file *model.File) error {
		if original != nil && original.State != file.State && file.State == model.FileImported {
			op.AtomicOpFromCtx(ctx).AddFrom(r.switchSlicesToImported(*file, now))
		}
		return nil
	})
}

func (r *Repository) validateSlicesOnFileStateTransition() {
	r.plugins.Collection().OnFileSave(func(ctx context.Context, now time.Time, original, file *model.File) error {
		if original != nil && original.State != file.State && file.State != model.FileClosing && file.State != model.FileImported {
			// FileClosing state is handled by the closeSliceOnFileClose method.
			// FileImported state is handled by the updateSlicesOnFileImport method.
			op.AtomicOpFromCtx(ctx).AddFrom(r.validateSliceStates(*file))
		}
		return nil
	})
}

func (r *Repository) stateTransition(k model.SliceKey, now time.Time, from, to model.SliceState, opts ...switchStateOption) *op.AtomicOp[model.Slice] {
	var file model.File
	var old, updated model.Slice
	return op.Atomic(r.client, &updated).
		// Read file
		Read(func(ctx context.Context) op.Op {
			return r.files.Get(k.FileKey).WithResultTo(&file)
		}).
		// Read entity for modification
		Read(func(ctx context.Context) op.Op {
			return r.Get(k).WithResultTo(&old)
		}).
		// Update the entity
		Write(func(ctx context.Context) op.Op {
			return r.switchState(ctx, file.State, old, now, from, to, opts...).SetResultTo(&updated)
		})
}

func (r *Repository) switchState(ctx context.Context, fileState model.FileState, oldValue model.Slice, now time.Time, from, to model.SliceState, opts ...switchStateOption) *op.TxnOp[model.Slice] {
	// Validate from state
	if oldValue.State != from {
		return op.ErrorTxn[model.Slice](errors.Errorf(`slice "%s" is in "%s" state, expected "%s"`, oldValue.SliceKey, oldValue.State, from))
	}

	// Validate file and slice state combination
	if err := validateFileAndSliceState(fileState, to); err != nil {
		return op.ErrorTxn[model.Slice](errors.PrefixErrorf(err, `unexpected slice "%s" state:`, oldValue.SliceKey))
	}

	// Switch slice state
	newValue, err := oldValue.WithState(now, to)
	if err != nil {
		return op.ErrorTxn[model.Slice](err)
	}

	// Process options
	for _, opt := range opts {
		opt(&newValue)
	}

	return r.save(ctx, now, &oldValue, &newValue)
}

func (r *Repository) switchStateInBatch(ctx context.Context, fileState model.FileState, original []model.Slice, now time.Time, from, to model.SliceState) *op.TxnOp[[]model.Slice] {
	var updated []model.Slice
	txn := op.TxnWithResult(r.client, &updated)
	for _, slice := range original {
		txn.Merge(r.switchState(ctx, fileState, slice, now, from, to).OnSucceeded(func(r *op.TxnResult[model.Slice]) {
			updated = append(updated, r.Result())
		}))
	}
	return txn
}

func (r *Repository) switchSlicesToImported(file model.File, now time.Time) *op.AtomicOp[[]model.Slice] {
	var slices, updated []model.Slice
	return op.Atomic(r.client, &updated).
		// Load slices
		Read(func(ctx context.Context) op.Op {
			return r.ListIn(file.FileKey).WithAllTo(&slices)
		}).
		// Close slices
		Write(func(ctx context.Context) op.Op {
			return r.switchStateInBatch(ctx, file.State, slices, now, model.SliceUploaded, model.SliceImported).SetResultTo(&updated)
		})
}

func (r *Repository) validateSliceStates(file model.File) *op.AtomicOp[[]model.Slice] {
	var slices, updated []model.Slice
	return op.Atomic(r.client, &updated).
		// Load slices
		Read(func(ctx context.Context) op.Op {
			return r.ListIn(file.FileKey).WithAllTo(&slices)
		}).
		// Validate slices states
		Write(func(ctx context.Context) op.Op {
			for _, slice := range slices {
				if err := validateFileAndSliceState(file.State, slice.State); err != nil {
					return op.ErrorOp(err)
				}
			}
			return nil
		})
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
