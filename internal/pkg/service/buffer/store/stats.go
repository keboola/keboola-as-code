package store

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
)

// sumStats sums all stats under the prefix.
func sumStats[T model.StatsProvider](ctx context.Context, client *etcd.Client, prefix iterator.DefinitionT[T]) (model.Stats, bool, error) {
	count := 0
	out := model.Stats{}
	err := prefix.
		Do(ctx, client).
		ForEachValue(func(item T, _ *iterator.Header) error {
			count++
			partialStats := item.GetStats()
			out.Count += partialStats.Count
			out.Size += partialStats.Size
			if partialStats.LastRecordAt.After(out.LastRecordAt) {
				out.LastRecordAt = partialStats.LastRecordAt
			}
			return nil
		})
	if err != nil {
		return out, false, err
	}

	if count == 0 {
		return out, false, nil
	}

	return out, true, nil
}
