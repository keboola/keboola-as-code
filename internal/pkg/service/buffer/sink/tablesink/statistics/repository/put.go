package repository

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Put creates or updates slices statistics records in the storage.LevelLocal.
func (r *Repository) Put(ctx context.Context, stats []statistics.PerSlice) (err error) {
	ctx, span := r.telemetry.Tracer().Start(ctx, "keboola.go.buffer.storage.statistics.Repository.Put")
	defer span.End(&err)

	var currentTxn *op.TxnOp
	var allTxn []*op.TxnOp
	addTxn := func() {
		currentTxn = op.NewTxnOp(r.client)
		allTxn = append(allTxn, currentTxn)
	}

	// Merge multiple put operations into one transaction
	i := 0
	for _, v := range stats {
		if i == 0 || i >= putMaxStatsPerTxn {
			i = 0
			addTxn()
		}
		currentTxn.Then(r.schema.InLevel(storage.LevelLocal).InSlice(v.SliceKey).Put(r.client, v.Value))
		i++
	}

	// Trace records and transactions count
	span.SetAttributes(
		attribute.Int("put.records_count", len(stats)),
		attribute.Int("put.txn_count", len(allTxn)),
	)

	// Run transactions in parallel
	wg := &sync.WaitGroup{}
	errs := errors.NewMultiError()
	for _, txn := range allTxn {
		txn := txn
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := txn.Do(ctx).Err(); err != nil {
				errs.Append(err)
			}
		}()
	}

	// Wait for all transactions
	wg.Wait()
	return errs.ErrorOrNil()
}
