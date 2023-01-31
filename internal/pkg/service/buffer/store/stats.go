package store

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
)

func SumStats[T model.StatsProvider](ctx context.Context, client *etcd.Client, prefix iterator.DefinitionT[T], out *model.Stats) error {
	return sumStatsOp(prefix, out).DoOrErr(ctx, client)
}

// sumStatsOp sums all stats from the iterator.
func sumStatsOp[T model.StatsProvider](prefix iterator.DefinitionT[T], out *model.Stats) *iterator.ForEachOpT[T] {
	return prefix.ForEachOp(func(item T, _ *iterator.Header) error {
		*out = out.Add(item.GetStats())
		return nil
	})
}
