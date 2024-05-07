package slice

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/diskalloc"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

func (r *Repository) openSlicesOnFileCreate() {
	r.plugins.Collection().OnFileSave(func(ctx context.Context, now time.Time, original, updated *model.File) {
		if original == nil {
			op.AtomicFromCtx(ctx).AddFrom(r.openSlicesForFile(now, *updated))
		}
	})
}

// openSlicesForFile creates new Slices, in the FileWriting state, for each assigned volume in the Sink.
func (r *Repository) openSlicesForFile(now time.Time, file model.File) *op.AtomicOp[[]model.Slice] {
	var newSlices []model.Slice
	return op.Atomic(r.client, &newSlices).
		WriteOrErr(func(ctx context.Context) (op.Op, error) {
			txn := op.Txn(r.client)
			for _, volumeID := range file.Assignment.Volumes {
				txn.Merge(r.openSlice(ctx, now, file, volumeID).OnSucceeded(func(r *op.TxnResult[model.Slice]) {
					newSlices = append(newSlices, r.Result())
				}))
			}
			return txn, nil
		})
}

func (r *Repository) openSlice(ctx context.Context, now time.Time, file model.File, volumeID volume.ID) *op.TxnOp[model.Slice] {
	// Create slice entity
	newSlice, err := r.newSlice(now, file, volumeID)
	if err != nil {
		return op.TxnWithError[model.Slice](err)
	}

	// Validate file state
	if err := validateFileAndSliceState(file.State, newSlice.State); err != nil {
		return op.TxnWithError[model.Slice](err)
	}

	// Pass allocation config to the statistics package, to estimate size of the new slice
	ctx = diskalloc.ContextWithConfig(ctx, file.LocalStorage.DiskAllocation)

	// Save new slice
	return r.save(ctx, now, nil, &newSlice)
}
