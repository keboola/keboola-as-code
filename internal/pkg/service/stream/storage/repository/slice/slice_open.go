package slice

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/state"
	"time"
)

func (r *Repository) openSlicesOnFileCreation() {
	r.plugins.Collection().OnFileSave(func(ctx context.Context, now time.Time, original, updated *model.File) {
		if original == nil {
			// Open slices
			op.AtomicFromCtx(ctx).AddFrom(r.openSlicesInFile(now, *updated))
		}
	})
}

func (r *Repository) openSlicesInFile(now time.Time, file model.File) *op.AtomicOp[[]model.Slice] {
	var newSlices []model.Slice
	return op.Atomic(r.client, &newSlices).
		WriteOrErr(func(ctx context.Context) (op.Op, error) {
			txn := op.Txn(r.client)
			for _, volumeID := range file.Assignment.Volumes {
				if t, err := r.openSlice(ctx, now, file, volumeID); err == nil {
					txn.Merge(t.OnSucceeded(func(r *op.TxnResult[model.Slice]) {
						newSlices = append(newSlices, r.Result())
					}))

				} else {
					return nil, err
				}
			}

			return txn, nil
		})
}

func (r *Repository) openSlice(ctx context.Context, now time.Time, file model.File, volumeID volume.ID) (*op.TxnOp[model.Slice], error) {
	// Create slice entity
	newSlice, err := NewSlice(now, file, volumeID)
	if err != nil {
		return nil, err
	}

	// Validate file state
	if err := state.ValidateFileAndSliceState(file.State, newSlice.State); err != nil {
		return nil, err
	}

	// Save new slice
	return r.save(ctx, now, nil, &newSlice), nil
}
