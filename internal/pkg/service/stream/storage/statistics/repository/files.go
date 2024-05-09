package repository

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/aggregate"
)

func (r *Repository) FilesStats(ctx context.Context, sinkKey key.SinkKey, first, last model.FileKey) *op.TxnOp[map[model.FileID]*statistics.Aggregated] {
	result := make(map[model.FileID]*statistics.Aggregated)
	txn := op.TxnWithResult(r.client, &result)
	for _, level := range level.AllLevels() {
		// Get stats prefix for the slice state
		pfx := r.schema.InLevel(level).InSink(sinkKey)

		// Sum
		txn.Then(
			pfx.GetAll(r.client, iterator.WithStartOffset(first.OpenedAt().String()), iterator.WithEndOffset(last.OpenedAt().String())).
				ForEach(func(v statistics.Value, header *iterator.Header) error {
					// TODO: how to get FileID?
					fileID := model.FileID{}
					aggregate.Aggregate(level, v, result[fileID])
					return nil
				}),
		)
	}
	return txn
}
