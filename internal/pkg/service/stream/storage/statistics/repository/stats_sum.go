package repository

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

// SumStats sums all stats from the iterator.
func SumStats(ctx context.Context, now time.Time, prefix iterator.DefinitionT[statistics.Value]) (out statistics.Value, err error) {
	var outReset statistics.Value
	if err := sumStatsOp(now, prefix, &out, &outReset).Do(ctx).Err(); err != nil {
		return out, err
	}
	return out.Add(outReset), nil
}

// sumStatsOp sums all stats from the iterator.
func sumStatsOp(now time.Time, prefix iterator.DefinitionT[statistics.Value], outSum *statistics.Value, outReset *statistics.Value) *iterator.ForEachT[statistics.Value] {
	outReset.ResetAt = ptr.Ptr(utctime.From(now))
	return prefix.ForEach(func(item statistics.Value, _ *iterator.Header) error {
		if item.ResetAt != nil {
			*outReset = outReset.Add(item)
		} else {
			*outSum = outSum.Add(item)
		}
		return nil
	})
}
