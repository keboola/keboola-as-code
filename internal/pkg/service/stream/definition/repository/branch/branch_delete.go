package branch

import (
	"context"
	"time"

	"github.com/keboola/go-utils/pkg/deepcopy"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

func (r *Repository) SoftDelete(k key.BranchKey, now time.Time, by definition.By) *op.AtomicOp[definition.Branch] {
	// Move entity from the active to the deleted prefix
	var original, deleted definition.Branch
	return op.Atomic(r.client, &deleted).
		// Read the entity
		Read(func(ctx context.Context) op.Op {
			return r.Get(k).WithResultTo(&original)
		}).
		// Mark deleted
		Write(func(ctx context.Context) op.Op {
			deleted = deepcopy.Copy(original).(definition.Branch)
			deleted.Delete(now, by, true)
			return r.save(ctx, now, by, &original, &deleted)
		})
}
