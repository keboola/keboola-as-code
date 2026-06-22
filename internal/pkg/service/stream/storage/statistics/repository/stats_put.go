package repository

import (
	"context"

	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

// Put creates or updates slices statistics records in the level.LevelLocal.
func (r *Repository) Put(ctx context.Context, nodeID string, stats []statistics.PerSlice) (err error) {
	ctx, span := r.telemetry.Tracer().Start(ctx, "keboola.go.stream.storage.statistics.Repository.Put")
	defer span.End(&err)

	// Trace records and transactions count
	span.SetAttributes(
		attribute.Int("put.records_count", len(stats)),
		attribute.Int("put.txn_count", (len(stats)+putMaxStatsPerTxn-1)/putMaxStatsPerTxn),
	)

	// Each Put is a single op, so batch by putMaxStatsPerTxn keys per transaction, run in parallel.
	return op.RunWriteTxnsInBatches(ctx, r.client, stats, putMaxStatsPerTxn, func(txn *op.TxnOp[op.NoResult], v statistics.PerSlice) {
		value := statistics.Value{
			FirstRecordAt:    v.FirstRecordAt,
			LastRecordAt:     v.LastRecordAt,
			RecordsCount:     v.RecordsCount,
			UncompressedSize: v.UncompressedSize,
			CompressedSize:   v.CompressedSize,
		}
		txn.Then(r.schema.InLevel(model.LevelLocal).InSliceSourceNode(v.SliceKey, nodeID).Put(r.client, value))
	})
}
