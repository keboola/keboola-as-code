package slice

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

// Rotate closes the active slice, if present, and opens a new slice in the same file volume.
//   - The old active slice, if present, is switched from the model.SliceWriting state to the model.SliceClosing state.
//   - New slice in the model.SliceWriting state is created.
//   - This method is used to rotate slice when the upload conditions are met.
func (r *Repository) Rotate(k model.SliceKey, now time.Time) *op.AtomicOp[model.Slice] {
	// Create atomic operation
	var opened model.Slice
	atomicOp := op.Atomic(r.client, &opened)

	// Load file to check the state
	var file model.File
	atomicOp.Read(func(ctx context.Context) op.Op {
		return r.files.Get(k.FileKey).WithResultTo(&file)
	})

	// Open a new slice
	atomicOp.Write(func(ctx context.Context) op.Op {
		return r.openSlice(ctx, now, file, k.VolumeID).SetResultTo(&opened)
	})

	// Close the active slice
	var activeSlices []model.Slice
	atomicOp.
		Read(func(ctx context.Context) op.Op {
			return r.ListInState(k.FileVolumeKey, model.SliceWriting).WithAllTo(&activeSlices)
		}).
		Write(func(ctx context.Context) op.Op {
			// There should be a maximum of one old slice in the model.SliceWriting state per each volume.
			// Log error and close all found files.
			if slicesCount := len(activeSlices); slicesCount > 1 {
				r.logger.Errorf(ctx, `unexpected state, found %d opened slices in the volume "%s"`, slicesCount, k)
			}

			return r.switchStateInBatch(ctx, file.State, activeSlices, now, model.SliceWriting, model.SliceClosing)
		})

	return atomicOp
}
