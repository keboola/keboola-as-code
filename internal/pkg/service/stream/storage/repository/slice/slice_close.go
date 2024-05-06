package slice

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"time"
)

func (r *Repository) closeSliceOnFileClose() {
	r.plugins.Collection().OnFileSave(func(ctx context.Context, now time.Time, original, updated *model.File) {
		if original != nil && original.State != updated.State && updated.State == model.FileClosing {
			op.AtomicFromCtx(ctx).AddFrom(r.closeSlicesInFile(now, *updated))
		}
	})
}

func (r *Repository) closeSlicesInFile(now time.Time, file model.File) *op.AtomicOp[[]model.Slice] {
	var original, updated []model.Slice
	return op.Atomic(r.client, &updated).
		// Load slices
		Read(func(ctx context.Context) op.Op {
			return r.ListInState(file.FileKey, model.SliceWriting).WithAllTo(&original)
		}).
		// Close slices
		WriteOrErr(func(ctx context.Context) (op.Op, error) {
			return r.switchStateInBatch(ctx, file.State, original, now, model.SliceWriting, model.SliceClosing)
		})
}
