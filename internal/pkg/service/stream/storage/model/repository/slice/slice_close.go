package slice

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

func (r *Repository) closeSliceOnFileClose() {
	r.plugins.Collection().OnFileClose(func(ctx context.Context, now time.Time, original, file *model.File) error {
		op.AtomicOpCtxFrom(ctx).AddFrom(r.closeSlicesInFile(*file, now))
		return nil
	})
}

// closeSlicesInFile all active slices, in the SliceWriting state, in the file.
// Slices are switched to the SliceClosing state.
func (r *Repository) closeSlicesInFile(file model.File, now time.Time) *op.AtomicOp[[]model.Slice] {
	var slices, closedSlices []model.Slice
	return op.Atomic(r.client, &closedSlices).
		// Load active slices
		Read(func(ctx context.Context) op.Op {
			return r.ListInState(file.FileKey, model.SliceWriting).WithAllTo(&slices)
		}).
		// Close active slices
		Write(func(ctx context.Context) op.Op {
			return r.
				switchStateInBatch(ctx, file.State, slices, now, model.SliceWriting, model.SliceClosing).
				SetResultTo(&closedSlices)
		})
}
