package repository

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/aggregate"
	"strings"
)

func (r *Repository) FilesStats(ctx context.Context, sinkKey key.SinkKey, first, last model.FileKey) *op.TxnOp[map[model.FileKey]*statistics.Aggregated] {
	result := make(map[model.FileKey]*statistics.Aggregated)
	txn := op.TxnWithResult(r.client, &result)
	for _, level := range level.AllLevels() {
		// Get stats prefix for the slice state
		pfx := r.schema.InLevel(level).InSink(sinkKey)

		// Sum
		txn.Then(
			pfx.GetAll(r.client, iterator.WithStartOffset(first.OpenedAt().String()), iterator.WithEndOffset(last.OpenedAt().String())).
				ForEachKV(func(kv *op.KeyValueT[statistics.Value], header *iterator.Header) error {
					// Extract fileID part from the key
					// It can be a method in the schema.go
					relativeKey := strings.TrimPrefix(kv.Key(), pfx.Prefix())
					openedAt, _, _ := strings.Cut(relativeKey, "/")
					fileKey := model.FileKey{SinkKey: sinkKey, FileID: model.FileID{OpenedAt: utctime.MustParse(openedAt)}}
					aggregate.Aggregate(level, kv.Value, result[fileKey])
					return nil
				}),
		)
	}
	return txn
}
