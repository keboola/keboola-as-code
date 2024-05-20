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
			softDeleteAllFrom(k, now, by, true).
			OnResult(func(r []definition.Sink) {
				if len(r) == 1 {
					deleted = r[0]
				}
			}))
}

func (r *Repository) deleteSinksOnSourceDelete() {
	r.plugins.Collection().OnSourceDelete(func(ctx context.Context, now time.Time, by definition.By, original, deleted *definition.Source) error {
		op.AtomicFromCtx(ctx).AddFrom(r.softDeleteAllFrom(deleted.SourceKey, now, by, false))
		return nil
	})
}

// softDeleteAllFrom the parent key.
func (r *Repository) softDeleteAllFrom(parentKey fmt.Stringer, now time.Time, by definition.By, deletedWithParent bool) *op.AtomicOp[[]definition.Sink] {
	var allOriginal, allDeleted []definition.Sink
	atomicOp := op.Atomic(r.client, &allDeleted)

	// Get or list
	switch k := parentKey.(type) {
	case key.SinkKey:
		atomicOp.Read(func(ctx context.Context) op.Op {
			return r.Get(k).WithOnResult(func(entity definition.Sink) { allOriginal = []definition.Sink{entity} })
		})
	default:
		atomicOp.Read(func(ctx context.Context) op.Op {
			return r.List(parentKey).WithAllTo(&allOriginal)
		})
	}

	// Iterate all
	atomicOp.Write(func(ctx context.Context) op.Op {
		txn := op.Txn(r.client)
		for _, old := range allOriginal {
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
