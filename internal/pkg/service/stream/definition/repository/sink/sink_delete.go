package sink

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-utils/pkg/deepcopy"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

func (r *Repository) SoftDelete(k key.SinkKey, now time.Time, by definition.By) *op.AtomicOp[definition.Sink] {
	var deleted definition.Sink
	return op.Atomic(r.client, &deleted).
		AddFrom(r.
			softDeleteAllFrom(k, now, by, false).
			OnResult(func(r []definition.Sink) {
				if len(r) == 1 {
					deleted = r[0]
				}
			}))
}

func (r *Repository) deleteSinksOnSourceDelete() {
	r.plugins.Collection().OnSourceDelete(func(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Source) {
		op.AtomicFromCtx(ctx).AddFrom(r.softDeleteAllFrom(updated.SourceKey, now, by, true))
	})
}

// softDeleteAllFrom the parent key.
func (r *Repository) softDeleteAllFrom(parentKey fmt.Stringer, now time.Time, by definition.By, deletedWithParent bool) *op.AtomicOp[[]definition.Sink] {
	var allOld, allDeleted []definition.Sink
	atomicOp := op.Atomic(r.client, &allDeleted)

	// Get or list
	switch k := parentKey.(type) {
	case key.SinkKey:
		atomicOp.ReadOp(r.Get(k).WithOnResult(func(entity definition.Sink) { allOld = []definition.Sink{entity} }))
	default:
		atomicOp.ReadOp(r.List(parentKey).WithAllTo(&allOld))
	}

	// Iterate all
	atomicOp.Write(func(ctx context.Context) op.Op {
		txn := op.Txn(r.client)
		for _, old := range allOld {
			old := old

			// Mark deleted
			deleted := deepcopy.Copy(old).(definition.Sink)
			deleted.Delete(now, by, deletedWithParent)

			// Save
			txn.Merge(r.save(ctx, now, by, &old, &deleted))
			allDeleted = append(allDeleted, deleted)
		}
		return txn
	})

	return atomicOp
}
