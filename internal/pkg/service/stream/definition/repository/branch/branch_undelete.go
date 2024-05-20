package branch

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

// Undelete a soft-deleted Branch and cascade undelete all nested Sources and Sinks,
// if they were deleted in cascade with the Branch (SoftDeletable.Deleted.Directly == false).
func (r *Repository) Undelete(k key.BranchKey, now time.Time, by definition.By) *op.AtomicOp[definition.Branch] {
	// Move entity from the deleted to the active prefix
	var created definition.Branch
	return op.Atomic(r.client, &created).
		// Check prerequisites
		Read(func(ctx context.Context) op.Op {
			return r.checkMaxBranchesPerProject(k.ProjectID, 1)
		}).
		// Read the entity
		Read(func(ctx context.Context) op.Op {
			return r.GetDeleted(k).WithResultTo(&created)
		}).
		// Mark undeleted
		Write(func(ctx context.Context) op.Op {
			created.Undelete(now, by)
			return r.save(ctx, now, by, nil, &created)
		})
}
