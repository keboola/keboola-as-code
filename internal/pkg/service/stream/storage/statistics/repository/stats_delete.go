package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
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

		// parentSumKey contains the sum of all statistics from the children that were deleted
		parentSumKey := r.schema.InLevel(model.LevelTarget).InParentOf(objectKey).Sum()

		// Get old value of parent sum
		var parentSum statistics.Value
		ops.Read(func(_ context.Context) op.Op {
			parentSum = statistics.Value{}
			return parentSumKey.GetOrEmpty(r.client).WithResultTo(&parentSum)
		})

		// Get statistics of the object
		var objectSum statistics.Value
		ops.Read(func(_ context.Context) op.Op {
			return objectPfx.GetAll(r.client).ForEach(func(item statistics.Value, _ *iterator.Header) error {
				objectSum = objectSum.With(item)
				return nil
			})
		})

		// Save updated parent sum
		ops.Write(func(_ context.Context) op.Op {
			if objectSum.RecordsCount > 0 {
				return parentSumKey.Put(r.client, parentSum.With(objectSum))
			}

			// Nop
			return nil
		})
	}

	return ops
}
