package store

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
)

// sumStatsOp sums all stats from the iterator.
func sumStatsOp[T model.StatsProvider](prefix iterator.DefinitionT[T], out *model.Stats) *iterator.ForEachOpT[T] {
	return prefix.ForEachOp(func(item T, _ *iterator.Header) error {
		*out = out.Add(item.GetStats())
		return nil
	})
}
