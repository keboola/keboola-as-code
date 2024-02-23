package repository

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

// Delete returns an etcd operation to delete all statistics associated with the object key.
// Statistics for the level.Target are not deleted but are rolled up to the parent object.
// This operation should not be used separately but atomically together with the deletion of the object.
func (r *Repository) Delete(objectKey fmt.Stringer) *op.AtomicOp[op.NoResult] {
	ops := op.Atomic(r.client, &op.NoResult{})
	for _, inLevel := range level.AllLevels() {
		// Object prefix contains all statistics related to the object
		objectPfx := r.schema.InLevel(inLevel).InObject(objectKey)

		// Keep statistics about successfully imported data in the parent object prefix, in the sum key
		if inLevel == level.Target {
			var objectSum statistics.Value
			var parentSum statistics.Value

			// sumKey contains the sum of all statistics from the children that were deleted
			sumKey := r.schema.InLevel(level.Target).InParentOf(objectKey).Sum()

			// Get sum from the parent object
			ops.ReadOp(sumKey.GetKV(r.client).WithOnResult(func(result *op.KeyValueT[statistics.Value]) {
				if result == nil {
					parentSum = statistics.Value{}
				} else {
					parentSum = result.Value
				}
			}))

			// Get statistics of the object
			ops.Read(func(context.Context) op.Op {
				objectSum = statistics.Value{}
				return SumStatsOp(objectPfx.GetAll(r.client), &objectSum)
			})

			// Save update sum
			ops.Write(func(ctx context.Context) op.Op {
				if objectSum.RecordsCount > 0 {
					return sumKey.Put(r.client, parentSum.Add(objectSum))
				} else {
					return nil
				}
			})
		}

		// Delete statistics
		ops.WriteOp(objectPfx.DeleteAll(r.client))
	}

	return ops
}
