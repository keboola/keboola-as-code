package branch

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"time"
)

func (r *Repository) Undelete(k key.BranchKey, now time.Time) *op.AtomicOp[definition.Branch] {
	// Move entity from the deleted to the active prefix
	var created definition.Branch
	return op.Atomic(r.client, &created).
		// Check prerequisites
		ReadOp(r.checkMaxBranchesPerProject(k.ProjectID, 1)).
		// Read the entity
		ReadOp(r.GetDeleted(k).WithResultTo(&created)).
		// Mark undeleted
		WriteOrErr(func(ctx context.Context) (op.Op, error) {
			created.Undelete(now)
			return r.saveOne(ctx, now, nil, &created)
		})
}
