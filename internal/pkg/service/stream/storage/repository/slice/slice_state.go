package slice

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"time"
)

func (r *Repository) stateTransitionWithFile() {
	r.plugins.Collection().OnFileSave(func(ctx context.Context, now time.Time, original, updated *model.File) {

		op.AtomicFromCtx(ctx)
	})
}

// StateTransition switch state of the file, state of the file slices is also atomically switched, if needed.
func (r *Repository) StateTransition(k model.SliceKey, now time.Time, from, to model.SliceState) *op.AtomicOp[model.Slice] {
	var file model.File
	return r.
		updateOne(k, now, func(slice model.Slice) (model.Slice, error) {
			return r.stateTransition(file.State, slice, now, from, to)
		}).
		ReadOp(r.files.Get(k.FileKey).WithResultTo(&file))
}

func (r *Repository) stateTransitionAllInFile(k model.FileKey, now time.Time, fileState model.FileState, from, to model.SliceState) *op.AtomicOp[[]model.Slice] {
	return r.updateAllInFile(k, now, func(slice model.Slice) (model.Slice, error) {
		return r.stateTransition(fileState, slice, now, from, to)
	})
}

func (r *Repository) stateTransition(fileState model.FileState, slice model.Slice, now time.Time, from, to model.SliceState) (model.Slice, error) {
	// Slice should be closed via one of the following ways:
	//   - Rotate/FileRepository.Rotate* methods - to create new replacement files
	//   - Close* methods - no replacement files are created.
	//   - Closing slice via StateTransition is therefore forbidden.
	if to == model.SliceClosing {
		return model.Slice{}, errors.Errorf(`unexpected transition to the state "%s", use Rotate or Close method`, model.SliceClosing)
	}

	// Validate from state
	if slice.State != from {
		return model.Slice{}, errors.Errorf(`slice "%s" is in "%s" state, expected "%s"`, slice.SliceKey, slice.State, from)
	}

	// Validate file and slice state combination
	if err := state.ValidateFileAndSliceState(fileState, to); err != nil {
		return slice, errors.PrefixErrorf(err, `unexpected slice "%s" state:`, slice.SliceKey)
	}

	// Switch slice state
	return slice.WithState(now, to)
}
