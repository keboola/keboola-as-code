package slice

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

func (r *Repository) deleteSlicesOnFileDelete() {
	r.plugins.Collection().OnFileSave(func(ctx context.Context, now time.Time, original, file *model.File) {
		if file.Deleted {
			op.AtomicFromCtx(ctx).AddFrom(r.deleteAll(file.FileKey, now))
		}
	})
}

// deleteAll slices from the file.
// This operation deletes only the metadata, the file resources in the local or staging storage are unaffected.
func (r *Repository) deleteAll(k model.FileKey, now time.Time) *op.AtomicOp[[]model.Slice] {
	var slices, deleted []model.Slice
	return op.Atomic(r.client, &deleted).
		ReadOp(r.ListIn(k).WithAllTo(&slices)).
		Write(func(ctx context.Context) op.Op {
			return r.updateAll(ctx, now, slices, func(slice model.Slice) (model.Slice, error) {
				slice.Deleted = true
				return slice, nil
			})
		})
}
