package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

func (r *Repository) rollupStatisticsOnFileDelete() {
	r.plugins.Collection().OnFileDelete(func(ctx context.Context, now time.Time, original, updated *model.File) error {
		op.AtomicOpFromCtx(ctx).AddFrom(r.deleteOrRollup(updated.FileKey))
		return nil
	})
}

// deleteOrRollup returns an etcd operation to delete all statistics associated with the object key.
// Statistics for the level.LevelTarget are not deleted but are rolled up to the parent object.
func (r *Repository) deleteOrRollup(objectKey fmt.Stringer) *op.AtomicOp[op.NoResult] {
	ops := op.Atomic(r.client, &op.NoResult{})
	for _, inLevel := range model.AllLevels() {
		// Object prefix contains all statistics related to the object
		objectPfx := r.schema.InLevel(inLevel).InObject(objectKey)

		// Delete statistics
		ops.Write(func(ctx context.Context) op.Op {
			return objectPfx.DeleteAll(r.client)
		})

		// Following rollup is only for the target level.
		// Keep statistics about successfully imported data in the parent object prefix, in the sum key.
		if inLevel != model.LevelTarget {
			continue
		}

		var objectSum statistics.Value
		var objectReset statistics.Value
		var parentSum statistics.Value
		var parentReset statistics.Value

		// sumKey contains the sum of all statistics from the children that were deleted
		sumKey := r.schema.InLevel(model.LevelTarget).InParentOf(objectKey).Sum()

		// resetKey contains the sum of all statistics from the children that are ignored
		resetKey := r.schema.InLevel(model.LevelTarget).InParentOf(objectKey).Reset()

		// Get sum from the parent object
		ops.Read(func(_ context.Context) op.Op {
			return sumKey.GetOrEmpty(r.client).WithResultTo(&parentSum)
		})

		// Get reset sum from the parent object
		ops.Read(func(_ context.Context) op.Op {
			return resetKey.GetOrEmpty(r.client).WithResultTo(&parentReset)
		})

		// Get statistics of the object
		ops.Read(func(_ context.Context) op.Op {
			objectSum = statistics.Value{}
			return sumStatsOp(r.clock.Now(), objectPfx.GetAll(r.client), &objectSum, &objectReset)
		})

		// Save update sum
		ops.Write(func(_ context.Context) op.Op {
			if objectSum.RecordsCount <= 0 {
				return nil
			}
			return sumKey.Put(r.client, parentSum.Add(objectSum))
		})

		// Save update reset
		ops.Write(func(_ context.Context) op.Op {
			if objectReset.RecordsCount <= 0 {
				return nil
			}
			return resetKey.Put(r.client, parentReset.Add(objectReset))
		})
	}

	return ops
}
