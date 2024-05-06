package slice

import (
	"context"
	"github.com/keboola/go-utils/pkg/deepcopy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"time"
)

func (r *Repository) deleteSlicesOnFileDelete() {
	r.plugins.Collection().OnFileSave(func(ctx context.Context, now time.Time, original, updated *model.File) {
		if updated.Deleted {
			op.AtomicFromCtx(ctx).AddFrom(r.deleteAll(updated.FileKey, now))
		}
	})
}

// Delete all slices from the file.
// This operation deletes only the metadata, the file resource in the staging storage is unaffected.
func (r *Repository) deleteAll(k model.FileKey, now time.Time) *op.AtomicOp[[]model.Slice] {
	var allOld, allDeleted []model.Slice
	return op.Atomic(r.client, &allDeleted).
		ReadOp(r.ListIn(k).WithAllTo(&allOld)).
		Write(func(ctx context.Context) op.Op {
			txn := op.Txn(r.client)
			for _, old := range allOld {
				// Mark deleted
				deleted := deepcopy.Copy(old).(model.Slice)
				deleted.Deleted = true

				// Save
				txn.Merge(r.save(ctx, now, &old, &deleted).OnSucceeded(func(r *op.TxnResult[model.Slice]) {
					allDeleted = append(allDeleted, r.Result())
				}))
			}
			return txn
		})
}
