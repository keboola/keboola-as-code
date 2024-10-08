package source

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-utils/pkg/deepcopy"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

func (r *Repository) SoftDelete(k key.SourceKey, now time.Time, by definition.By) *op.AtomicOp[definition.Source] {
	var deleted definition.Source
	return op.Atomic(r.client, &deleted).
		AddFrom(r.
			softDeleteAllFrom(k, now, by, true).
			OnResult(func(r []definition.Source) {
				if len(r) == 1 {
					deleted = r[0]
				}
			}))
}

func (r *Repository) deleteSourcesOnBranchDelete() {
	r.plugins.Collection().OnBranchDelete(func(ctx context.Context, now time.Time, by definition.By, original, deleted *definition.Branch) error {
		op.AtomicOpCtxFrom(ctx).AddFrom(r.softDeleteAllFrom(deleted.BranchKey, now, by, false))
		return nil
	})
}

// softDeleteAllFrom the parent key.
func (r *Repository) softDeleteAllFrom(parentKey fmt.Stringer, now time.Time, by definition.By, directly bool) *op.AtomicOp[[]definition.Source] {
	var allOriginal, allDeleted []definition.Source
	atomicOp := op.Atomic(r.client, &allDeleted)

	// Get or list
	switch k := parentKey.(type) {
	case key.SourceKey:
		atomicOp.Read(func(ctx context.Context) op.Op {
			return r.Get(k).WithOnResult(func(entity definition.Source) { allOriginal = []definition.Source{entity} })
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
			// Mark deleted
			deleted := deepcopy.Copy(old).(definition.Source)
			deleted.Delete(now, by, directly)

			// Save
			txn.Merge(r.save(ctx, now, by, &old, &deleted))
			allDeleted = append(allDeleted, deleted)
		}
		return txn
	})

	return atomicOp
}
