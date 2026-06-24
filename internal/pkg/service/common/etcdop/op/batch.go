package op

import (
	"context"
	"sync"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// MaxOpsPerTxn is the safe upper bound of operations in a single etcd transaction.
//
// It is kept well under the ETCD_MAX_TXN_OPS limit (1024 in both production and dev/compose),
// leaving headroom for IF conditions and nested operations. Callers that write more than one
// operation per item must divide this by the ops-per-item to derive the batch size
// (see RunWriteTxnsInBatches).
const MaxOpsPerTxn = 100

// RunWriteTxnsInBatches splits items into batches of at most maxItemsPerTxn, builds one
// transaction per batch via addToTxn, and executes the transactions in parallel.
//
// Unlike AtomicOp, there is NO cross-batch atomicity and no read-revision guard: each
// batch is an independent transaction. Use this only for bulk writes/deletes that
// tolerate partial progress, where a partially applied result is never user-visibly
// inconsistent (e.g. cascade soft-deletes, statistics rollup/reset).
//
// The op count of each transaction is bounded by maxItemsPerTxn * (ops added per item),
// independent of len(items), so it cannot exceed the etcd per-transaction limit.
//
// Note: one goroutine per batch with no concurrency cap, matching the original stats_put
// behaviour. Add a bounded worker pool if batch counts ever grow large enough to overload etcd.
func RunWriteTxnsInBatches[T any](ctx context.Context, client etcd.KV, items []T, maxItemsPerTxn int, addToTxn func(txn *TxnOp[NoResult], item T)) error {
	if maxItemsPerTxn < 1 {
		maxItemsPerTxn = 1
	}

	// Group items into per-batch transactions
	var batches []*TxnOp[NoResult]
	for i, item := range items {
		if i%maxItemsPerTxn == 0 {
			batches = append(batches, Txn(client))
		}
		addToTxn(batches[len(batches)-1], item)
	}

	// Run batches in parallel, collect all errors
	wg := &sync.WaitGroup{}
	errs := errors.NewMultiError()
	for _, txn := range batches {
		wg.Go(func() {
			if err := txn.Do(ctx).Err(); err != nil {
				errs.Append(err)
			}
		})
	}
	wg.Wait()

	return errs.ErrorOrNil()
}
