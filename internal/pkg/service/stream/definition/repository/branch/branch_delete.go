package branch

import (
	"context"
	"github.com/keboola/go-utils/pkg/deepcopy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"time"
)

func (r *Repository) SoftDelete(k key.BranchKey, now time.Time) *op.AtomicOp[definition.Branch] {
	// Move entity from the active to the deleted prefix
	var old, updated definition.Branch
	return op.Atomic(r.client, &updated).
		// Read the entity
		ReadOp(r.Get(k).WithResultTo(&old)).
		// Mark deleted
		WriteOrErr(func(ctx context.Context) (op.Op, error) {
			updated = deepcopy.Copy(old).(definition.Branch)
			updated.Delete(now, false)
			return r.saveOne(ctx, now, &old, &updated)
		})
}
