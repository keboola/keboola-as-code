package repository

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

func (r *Repository) ResetAllSinksStats(ctx context.Context, sinkKeys []key.SinkKey) error {
	ops := op.Atomic(r.client, &op.NoResult{})

	for _, sinkKey := range sinkKeys {
		ops.AddFrom(r.ResetSinkStats(sinkKey))
	}

	return ops.Do(ctx).Err()
}

// ResetSinkStats sums all statistics for data in target level and saves the sum in _reset key.
// Local and staging levels are unaffected because those data will be moved to target later.
func (r *Repository) ResetSinkStats(sinkKey key.SinkKey) *op.AtomicOp[op.NoResult] {
	ops := op.Atomic(r.client, &op.NoResult{})

	// Object prefix contains all statistics related to the object
	objectPfx := r.schema.InLevel(model.LevelTarget).InObject(sinkKey)

	var objectSum statistics.Value

	// resetKey contains the sum of all statistics from the children that were deleted
	resetKey := r.schema.InLevel(model.LevelTarget).InObject(sinkKey).Reset()

	// Get statistics of the object - exclude actual reset value
	ops.Read(func(context.Context) op.Op {
		return objectPfx.GetAll(r.client).ForEach(func(item statistics.Value, _ *iterator.Header) error {
			if item.ResetAt == nil {
				objectSum = objectSum.With(item)
			}
			return nil
		})
	})

	// Save reset key
	ops.Write(func(context.Context) op.Op {
		objectSum.ResetAt = ptr.Ptr(utctime.From(r.clock.Now()))
		// Sum aggregated and non-aggregated statistics
		return resetKey.Put(r.client, objectSum)
	})

	return ops
}

func (r *Repository) LastReset(sinkKey key.SinkKey) op.WithResult[statistics.Value] {
	return r.schema.InLevel(model.LevelTarget).InObject(sinkKey).Reset().GetOrEmpty(r.client)
}
