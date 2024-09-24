package slice

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

func (r *Repository) openSlicesOnFileOpen() {
	// Note: Don't use OnFileOpen here, we need the final file, with all fields set.
	// This must be called after all OnFileOpen callbacks.
	r.plugins.Collection().OnFileSave(func(ctx context.Context, now time.Time, original, file *model.File) error {
		if original == nil {
			op.AtomicOpCtxFrom(ctx).AddFrom(r.openSlicesForFile(*file, now))
		}
		return nil
	})
}

// openSlicesForFile creates new Slices, in the FileWriting state, for each assigned volume in the Sink.
func (r *Repository) openSlicesForFile(file model.File, now time.Time) *op.AtomicOp[[]model.Slice] {
	var newSlices []model.Slice
	return op.Atomic(r.client, &newSlices).
		Write(func(ctx context.Context) op.Op {
			txn := op.Txn(r.client)
			for _, volumeID := range file.LocalStorage.Assignment.Volumes {
				txn.Merge(r.openSlice(ctx, now, file, volumeID).OnSucceeded(func(r *op.TxnResult[model.Slice]) {
					newSlices = append(newSlices, r.Result())
				}))
			}
			return txn
		})
}

func (r *Repository) openSlice(ctx context.Context, now time.Time, file model.File, volumeID volume.ID) *op.TxnOp[model.Slice] {
	// Create slice entity
	newSlice, err := r.newSlice(now, file, volumeID)
	if err != nil {
		return op.ErrorTxn[model.Slice](err)
	}

	// Validate file state
	if err := validateFileAndSliceState(file.State, newSlice.State); err != nil {
		return op.ErrorTxn[model.Slice](err)
	}

	// Call plugins
	if err := r.plugins.Executor().OnSliceOpen(ctx, now, file, &newSlice); err != nil {
		return op.ErrorTxn[model.Slice](err)
	}

	// Save new slice
	return r.save(ctx, now, nil, &newSlice)
}
