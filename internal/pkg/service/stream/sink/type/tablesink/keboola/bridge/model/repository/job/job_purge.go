package job

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-utils/pkg/deepcopy"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model"
)

func (r *Repository) purgeJobsOnSinkDelete() {
	r.plugins.Collection().OnSinkDelete(func(ctx context.Context, now time.Time, by definition.By, original, deleted *definition.Sink) error {
		op.AtomicOpCtxFrom(ctx).AddFrom(r.purgeAllFrom(deleted.SinkKey))
		return nil
	})
}

// purgeAllFrom the parent key (SinkKey, SourceKey, BranchKey, ProjectKey).
func (r *Repository) purgeAllFrom(parentKey fmt.Stringer) *op.AtomicOp[[]model.Job] {
	var allOriginal, allDeleted []model.Job
	atomicOp := op.Atomic(r.client, &allDeleted)

	// List by SinkKey
	atomicOp.Read(func(ctx context.Context) op.Op {
		return r.List(parentKey).WithAllTo(&allOriginal)
	})

	// Iterate all
	atomicOp.Write(func(ctx context.Context) op.Op {
		txn := op.Txn(r.client)
		for _, old := range allOriginal {
			// Purge job
			purged := deepcopy.Copy(old).(model.Job)
			purged.Deleted = true

			txn.Merge(r.save(&purged))
			allDeleted = append(allDeleted, purged)
		}
		return txn
	})

	return atomicOp
}

// Purge purges the job.
func (r *Repository) Purge(input *model.Job) *op.AtomicOp[model.Job] {
	k := input.JobKey
	var deleted model.Job
	return op.Atomic(r.client, &deleted).
		// Entity must exist
		Read(func(ctx context.Context) op.Op {
			return r.ExistsOrErr(k)
		}).
		// Delete
		Write(func(ctx context.Context) op.Op {
			// entity to be deleted
			deleted = deepcopy.Copy(*input).(model.Job)
			deleted.Deleted = true
			return r.save(&deleted)
		}).
		// Update the input entity, if the operation is successful
		OnResult(func(result model.Job) {
			*input = result
		})
}
