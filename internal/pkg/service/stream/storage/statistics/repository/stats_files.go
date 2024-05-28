package repository

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/aggregate"
)

func (r *Repository) FilesStats(sinkKey key.SinkKey, start model.FileID, end model.FileID) *op.TxnOp[map[model.FileID]*statistics.Aggregated] {
	result := make(map[model.FileID]*statistics.Aggregated)
	txn := op.TxnWithResult(r.client, &result)

	for _, l := range level.AllLevels() {
		// Get stats prefix for the slice state
		pfx := r.schema.InLevel(l).InSink(sinkKey)

		opts := []iterator.Option{
			iterator.WithStartOffset(start.OpenedAt.String(), true),
			iterator.WithEndOffset(end.OpenedAt.String(), true),
		}

		// Sum
		txn.Then(
			pfx.GetAll(r.client, opts...).
				ForEachKV(func(kv *op.KeyValueT[statistics.Value], header *iterator.Header) error {
					// Extract fileID part from the key
					fileID := model.NewFileIDFromKey(kv.Key(), pfx.Prefix())
					if result[fileID] == nil {
						result[fileID] = &statistics.Aggregated{}
					}
					aggregate.Aggregate(l, kv.Value, result[fileID])
					return nil
				}),
		)
	}
	return txn
}
