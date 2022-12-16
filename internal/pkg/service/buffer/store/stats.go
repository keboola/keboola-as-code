package store

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

func (s *Store) UpdateSliceStats(ctx context.Context, nodeID string, stats []model.SliceStats) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.configstore.UpdateStats")
	defer telemetry.EndSpan(span, &err)

	var ops []op.Op
	for _, v := range stats {
		ops = append(ops, s.updateStatsOp(ctx, nodeID, v))
	}
	_, err = op.MergeToTxn(ctx, ops...).Do(ctx, s.client)
	return err
}

func (s *Store) updateStatsOp(_ context.Context, nodeID string, stats model.SliceStats) op.NoResultOp {
	return s.schema.
		SliceStats().
		InSlice(stats.SliceKey).
		ByNodeID(nodeID).
		Put(stats)
}
