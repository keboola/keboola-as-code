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

// Undelete a soft-deleted Sink.
func (r *Repository) Undelete(k key.SinkKey, now time.Time, by definition.By) *op.AtomicOp[definition.Sink] {
	var undeleted definition.Sink
	return op.Atomic(r.client, &undeleted).
		// Check prerequisites
		Read(func(ctx context.Context) op.Op {
			return r.sources.ExistsOrErr(k.SourceKey)
		}).
		// Read the entity
		Read(func(ctx context.Context) op.Op {
			return r.checkMaxSinksPerSource(k.SourceKey, 1)
		}).
		// Mark undeleted
		AddFrom(r.
			undeleteAllFrom(k, now, by, true).
			OnResult(func(r []definition.Sink) {
				if len(r) == 1 {
					undeleted = r[0]
				}
			}))
}

func (r *Repository) undeleteSinksOnSourceUndelete() {
	r.plugins.Collection().OnSourceUndelete(func(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Source) error {
		op.AtomicFromCtx(ctx).AddFrom(r.undeleteAllFrom(updated.SourceKey, now, by, false))
		return nil
	})
}

// undeleteAllFrom the parent key.
func (r *Repository) undeleteAllFrom(parentKey fmt.Stringer, now time.Time, by definition.By, directly bool) *op.AtomicOp[[]definition.Sink] {
	var allOld, allCreated []definition.Sink
	atomicOp := op.Atomic(r.client, &allCreated)

	// Get or list
	switch k := parentKey.(type) {
	case key.SinkKey:
		atomicOp.Read(func(ctx context.Context) op.Op {
			return r.GetDeleted(k).WithOnResult(func(entity definition.Sink) { allOld = []definition.Sink{entity} })
		})
	default:
		atomicOp.Read(func(ctx context.Context) op.Op {
			return r.ListDeleted(parentKey).WithAllTo(&allOld)
		})
	}

	// Iterate all
	atomicOp.Write(func(ctx context.Context) op.Op {
		allCreated = nil
		txn := op.Txn(r.client)
		for _, old := range allOld {
			if old.Deleted.Directly != directly {
				continue
			}

			// Mark undeleted
			created := deepcopy.Copy(old).(definition.Sink)
			created.Undelete(now, by)

			// Create a new version record, if the entity has been undeleted manually
			if directly {
				versionDescription := fmt.Sprintf(`Undeleted to version "%d".`, old.Version.Number)
				created.IncrementVersion(created, now, by, versionDescription)
			}

			// Save
			txn.Merge(r.save(ctx, now, by, nil, &created))
			allCreated = append(allCreated, created)
		}
		return txn
	})

	return atomicOp
}
