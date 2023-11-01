package repository

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
)

// SumStats sums all stats from the iterator.
func SumStats(ctx context.Context, prefix iterator.DefinitionT[statistics.Value]) (out statistics.Value, err error) {
	if err := SumStatsOp(prefix, &out).Do(ctx).Err(); err != nil {
		return out, err
	}
	return out, nil
}

// SumStatsOp sums all stats from the iterator.
func SumStatsOp(prefix iterator.DefinitionT[statistics.Value], out *statistics.Value) *iterator.ForEachOpT[statistics.Value] {
	return prefix.ForEachOp(func(item statistics.Value, _ *iterator.Header) error {
		*out = out.Add(item)
		return nil
	})
}
