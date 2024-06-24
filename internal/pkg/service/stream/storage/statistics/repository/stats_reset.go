package repository

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

// ResetStats sums all statistics for data in target level and saves the sum in _reset key.
// Local and staging levels are unaffected because those data will be moved to target later.
func (r *Repository) ResetStats(objectKey fmt.Stringer) *op.AtomicOp[op.NoResult] {
	ops := op.Atomic(r.client, &op.NoResult{})

	// Object prefix contains all statistics related to the object
	objectPfx := r.schema.InLevel(model.LevelTarget).InObject(objectKey)

	var objectSum statistics.Value
	var resetSum statistics.Value

	// resetKey contains the sum of all statistics from the children that were deleted
	resetKey := r.schema.InLevel(model.LevelTarget).InObject(objectKey).Reset()

	// Get statistics of the object
	ops.Read(func(context.Context) op.Op {
		objectSum = statistics.Value{}
		return sumStatsOp(objectPfx.GetAll(r.client), &objectSum, &resetSum)
	})

	// resetSum is intentionally ignored

	// Save reset key
	ops.Write(func(context.Context) op.Op {
		objectSum.Reset = true
		// Sum aggregated and non-aggregated statistics
		return resetKey.Put(r.client, objectSum)
	})

	return ops
}
