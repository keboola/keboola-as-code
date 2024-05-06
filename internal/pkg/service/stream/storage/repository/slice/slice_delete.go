package slice

import (
	"github.com/keboola/go-utils/pkg/deepcopy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"time"
)

// Delete the slice.
// This operation deletes only the metadata, the file resource in the staging storage is unaffected.
func (r *Repository) deleteAllFrom(k model.FileKey, now time.Time) *op.AtomicOp[[]model.Slice] {
	var allOld, allDeleted []model.Slice
	return op.Atomic(r.client, &allDeleted).
		ReadOp(r.ListIn(k).WithAllTo(&allOld)).
		Write(func(ctx context.Context) op.Op {
			txn := op.Txn(r.client)
			for _, old := range allOld {
				old := old

				// Mark deleted
				deleted := deepcopy.Copy(old).(model.Slice)
				deleted.Deleted = true

				// Save
				r.save(ctx, now, &old, &deleted)
				allDeleted = append(allDeleted, deleted)
			}
			return txn
		})
}
