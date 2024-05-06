package slice

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"time"
)

// Rotate closes the opened slice, if present, and opens a new slice in the file volume.
//   - THE NEW SLICE is ALWAYS created in the state storage.SliceWriting.
//   - THE OLD SLICE in the storage.SliceWriting state, IF PRESENT, is switched to the storage.SliceClosing state.
//   - If no old slice exists, this operation effectively corresponds to the Open operation.
//   - Slices rotation is done atomically.
//   - This method is used to rotate slices when the upload conditions are met.
func (r *Repository) Rotate(now time.Time, k model.SliceKey) *op.AtomicOp[model.Slice] {
	// Create atomic operation
	var opened model.Slice
	atomicOp := op.Atomic(r.client, &opened)

	// Load file to check the state
	var file model.File
	atomicOp.Read(func(ctx context.Context) op.Op {
		return r.files.Get(k.FileKey).WithResultTo(&file)
	})

	// Open a new slice
	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
		if txn, err := r.openSlice(ctx, now, file, k.VolumeID); err == nil {
			return txn.OnSucceeded(func(r *op.TxnResult[model.Slice]) {
				opened = r.Result()
			}), nil
		} else {
			return nil, err
		}
	})

	// Close the active slice, on the volume
	var activeSlices []model.Slice
	atomicOp.
		Read(func(ctx context.Context) op.Op {
			return r.ListInState(k.FileVolumeKey, model.SliceWriting).WithAllTo(&activeSlices)
		}).
		WriteOrErr(func(ctx context.Context) (op.Op, error) {
			// There should be a maximum of one old slice in the model.SliceWriting state per each volume.
			// Log error and close all found files.
			if slicesCount := len(activeSlices); slicesCount > 1 {
				r.logger.Errorf(ctx, `unexpected state, found %d opened slices in the volume "%s"`, slicesCount, k)
			}

			return r.switchStateInBatch(ctx, file.State, activeSlices, now, model.SliceWriting, model.SliceClosing)
		})

	return atomicOp
}
