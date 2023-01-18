package store

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

func GetStatsFrom[T model.StatsProvider](ctx context.Context, s *Store, prefix iterator.DefinitionT[T]) (out model.Stats, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.GetStats")
	defer telemetry.EndSpan(span, &err)
	err = sumStatsOp(prefix, &out).DoOrErr(ctx, s.client)
	return out, err
}

// sumStatsOp sums all stats from the iterator.
func sumStatsOp[T model.StatsProvider](prefix iterator.DefinitionT[T], out *model.Stats) *iterator.ForEachOpT[T] {
	return prefix.ForEachOp(func(item T, _ *iterator.Header) error {
		partial := item.GetStats()
		out.RecordsCount += partial.RecordsCount
		out.RecordsSize += partial.RecordsSize
		out.BodySize += partial.BodySize
		out.FileSize += partial.FileSize
		out.FileGZipSize += partial.FileGZipSize
		if partial.LastRecordAt.After(out.LastRecordAt) {
			out.LastRecordAt = partial.LastRecordAt
		}
		return nil
	})
}
