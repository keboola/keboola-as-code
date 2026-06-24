package repository

import (
	"context"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// ResetAllSinksStats resets statistics of each sink in its own bounded transaction, run in parallel.
//
// Each sink reset is an independent atomic operation, so the per-transaction op count stays bounded
// regardless of the number of sinks (previously all sinks were merged into one transaction, which
// exceeded the etcd per-transaction operation limit). There is no cross-sink atomicity, which is
// acceptable: a partially reset set of sinks is never user-visibly inconsistent and re-running
// completes it.
func (r *Repository) ResetAllSinksStats(ctx context.Context, sinkKeys []key.SinkKey) error {
	wg := &sync.WaitGroup{}
	errs := errors.NewMultiError()
	for _, sinkKey := range sinkKeys {
		wg.Go(func() {
			if err := r.ResetSinkStats(sinkKey).Do(ctx).Err(); err != nil {
				// Prefix with the sink key so a failure can be attributed when multiple sinks reset concurrently.
				errs.Append(errors.PrefixErrorf(err, `sink "%s"`, sinkKey.String()))
			}
		})
	}
	wg.Wait()

	return errs.ErrorOrNil()
}

// ResetSinkStats sums all statistics for data in target level and saves the sum in _reset key.
// Local and staging levels are unaffected because those data will be moved to target later.
func (r *Repository) ResetSinkStats(sinkKey key.SinkKey) *op.AtomicOp[op.NoResult] {
	// SkipPrefixKeysCheck: the read sums all statistics under the sink prefix, which may contain
	// many per-slice keys. Without this, one IF condition is generated per key, exploding the
	// transaction op count. Modifications to the prefix are still detected; only concurrent
	// deletion of an individual key is not — harmless for a reset that just re-sums the prefix.
	ops := op.Atomic(r.client, &op.NoResult{}).SkipPrefixKeysCheck()

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
		objectSum.ResetAt = new(utctime.From(r.clock.Now()))
		// Sum aggregated and non-aggregated statistics
		return resetKey.Put(r.client, objectSum)
	})

	return ops
}

func (r *Repository) LastReset(sinkKey key.SinkKey) op.WithResult[statistics.Value] {
	return r.schema.InLevel(model.LevelTarget).InObject(sinkKey).Reset().GetOrEmpty(r.client)
}
