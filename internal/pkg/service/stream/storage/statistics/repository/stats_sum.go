package repository

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

// SumStats sums all stats from the iterator.
func SumStats(ctx context.Context, prefix iterator.DefinitionT[statistics.Value]) (out statistics.Value, err error) {
	var outReset statistics.Value
	if err := sumStatsOp(prefix, &out, &outReset).Do(ctx).Err(); err != nil {
		return out, err
	}
	return out.Add(outReset), nil
}

// sumStatsOp sums all stats from the iterator.
func sumStatsOp(prefix iterator.DefinitionT[statistics.Value], outSum *statistics.Value, outReset *statistics.Value) *iterator.ForEachT[statistics.Value] {
	outReset.Reset = true
	return prefix.ForEach(func(item statistics.Value, _ *iterator.Header) error {
		if item.Reset {
			*outReset = outReset.Add(item)
		} else {
			*outSum = outSum.Add(item)
		}
		return nil
	})
}
