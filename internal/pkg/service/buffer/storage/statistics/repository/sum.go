package repository

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	etcd "go.etcd.io/etcd/client/v3"
)

func SumStats(ctx context.Context, client *etcd.Client, prefix iterator.DefinitionT[Value]) (out Value, err error) {
	if err := SumStatsOp(prefix, &out).DoOrErr(ctx, client); err != nil {
		return out, err
	}
	return out, nil
}

// SumStatsOp sums all stats from the iterator.
func SumStatsOp(prefix iterator.DefinitionT[Value], out *Value) *iterator.ForEachOpT[Value] {
	return prefix.ForEachOp(func(item Value, _ *iterator.Header) error {
		*out = out.Add(item)
		return nil
	})
}
