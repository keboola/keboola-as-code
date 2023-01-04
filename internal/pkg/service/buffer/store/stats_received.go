package store

import (
	"context"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const maxStatsPerTxn = 50

func (s *Store) GetReceivedStatsByFile(ctx context.Context, fileKey key.FileKey) (stats model.Stats, found bool, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.GetReceivedStatsByFile")
	defer telemetry.EndSpan(span, &err)
	return sumStats(ctx, s.client, s.schema.ReceivedStats().InFile(fileKey).GetAll())
}

func (s *Store) GetReceivedStatsBySlice(ctx context.Context, sliceKey key.SliceKey) (stats model.Stats, found bool, err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.GetReceivedStatsBySlice")
	defer telemetry.EndSpan(span, &err)
	return sumStats(ctx, s.client, s.schema.ReceivedStats().InSlice(sliceKey).GetAll())
}

func (s *Store) UpdateSliceReceivedStats(ctx context.Context, nodeID string, stats []model.SliceStats) (err error) {
	_, span := s.tracer.Start(ctx, "keboola.go.buffer.store.UpdateSliceReceivedStats")
	defer telemetry.EndSpan(span, &err)

	var currentTxn *op.TxnOp
	var allTxn []*op.TxnOp
	addTxn := func() {
		currentTxn = op.NewTxnOp()
		allTxn = append(allTxn, currentTxn)
	}

	i := 0
	for _, v := range stats {
		if i == 0 || i >= maxStatsPerTxn {
			i = 0
			addTxn()
		}
		currentTxn.Then(s.updateStatsOp(ctx, nodeID, v))
		i++
	}

	wg := &sync.WaitGroup{}
	errs := errors.NewMultiError()
	for _, txn := range allTxn {
		txn := txn
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := txn.Do(ctx, s.client); err != nil {
				errs.Append(err)
			}
		}()
	}
	wg.Wait()
	return errs.ErrorOrNil()
}

func (s *Store) updateStatsOp(_ context.Context, nodeID string, stats model.SliceStats) op.NoResultOp {
	return s.schema.
		ReceivedStats().
		InSlice(stats.SliceKey).
		ByNodeID(nodeID).
		Put(stats)
}

func (s *Store) deleteReceiverStatsOp(_ context.Context, receiverKey key.ReceiverKey) op.CountOp {
	return s.schema.
		ReceivedStats().
		InReceiver(receiverKey).
		DeleteAll()
}
