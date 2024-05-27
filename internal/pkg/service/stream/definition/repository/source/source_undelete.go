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

// Undelete a soft-deleted Source and cascade undelete all nested Sinks,
// if they were deleted in cascade with the Source (SoftDeletable.Deleted.Directly == false).
func (r *Repository) Undelete(k key.SourceKey, now time.Time, by definition.By) *op.AtomicOp[definition.Source] {
	var undeleted definition.Source
	return op.Atomic(r.client, &undeleted).
		// Check prerequisites
		Read(func(ctx context.Context) op.Op {
			return r.branches.ExistsOrErr(k.BranchKey)
		}).
		// Read the entity
		Read(func(ctx context.Context) op.Op {
			return r.checkMaxSourcesPerBranch(k.BranchKey, 1)
		}).
		// Mark undeleted
		AddFrom(r.
			undeleteAllFrom(k, now, by, true).
			OnResult(func(r []definition.Source) {
				if len(r) == 1 {
					undeleted = r[0]
				}
			}))
}

func (r *Repository) undeleteSourcesOnBranchUndelete() {
	r.plugins.Collection().OnBranchUndelete(func(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Branch) error {
		op.AtomicOpFromCtx(ctx).AddFrom(r.undeleteAllFrom(updated.BranchKey, now, by, false))
		return nil
	})
}

// undeleteAllFrom the parent key.
func (r *Repository) undeleteAllFrom(parentKey fmt.Stringer, now time.Time, by definition.By, directly bool) *op.AtomicOp[[]definition.Source] {
	var allOld, allCreated []definition.Source
	atomicOp := op.Atomic(r.client, &allCreated)

	// Get or list
	switch k := parentKey.(type) {
	case key.SourceKey:
		atomicOp.Read(func(ctx context.Context) op.Op {
			return r.GetDeleted(k).WithOnResult(func(entity definition.Source) { allOld = []definition.Source{entity} })
		})
	default:
		atomicOp.Read(func(ctx context.Context) op.Op {
			return r.ListDeleted(parentKey).WithAllTo(&allOld)
		})
	}

	// Iterate all
	atomicOp.Write(func(ctx context.Context) op.Op {
		txn := op.Txn(r.client)
		for _, old := range allOld {
			old := old

			if old.Deleted.Directly != directly {
				continue
			}

			// Mark undeleted
			created := deepcopy.Copy(old).(definition.Source)
			created.Undelete(now, by)

			// Create a new version record, if the entity has been undeleted directly
			if directly {
				versionDescription := fmt.Sprintf(`Undeleted to version "%d".`, old.Version.Number)
				created.IncrementVersion(created, now, by, versionDescription)
			}

			// Save
			txn.Merge(r.save(ctx, now, by, &old, &created))
			allCreated = append(allCreated, created)
		}
		return txn
	})

	return atomicOp
}
