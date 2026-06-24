package op_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

// txnCountingKV counts how many low-level operations (one per executed transaction) are sent to etcd.
type txnCountingKV struct {
	etcd.KV
	count atomic.Int64
}

func (kv *txnCountingKV) Do(ctx context.Context, op etcd.Op) (etcd.OpResponse, error) {
	kv.count.Add(1)
	return kv.KV.Do(ctx, op)
}

// TestRunWriteTxnsInBatches verifies that the helper splits items into the expected number of
// transactions, independent of the etcd ETCD_MAX_TXN_OPS limit (it asserts the transaction COUNT,
// not whether a single big transaction would be rejected).
func TestRunWriteTxnsInBatches(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	cases := []struct {
		items     int
		maxPerTxn int
		wantTxns  int64
	}{
		{items: 0, maxPerTxn: 50, wantTxns: 0},   // nothing to do, no transaction
		{items: 1, maxPerTxn: 50, wantTxns: 1},   // single partial batch
		{items: 50, maxPerTxn: 50, wantTxns: 1},  // exactly one full batch
		{items: 51, maxPerTxn: 50, wantTxns: 2},  // one full + one partial batch
		{items: 70, maxPerTxn: 50, wantTxns: 2},  // matches the slice-delete cascade scenario
		{items: 150, maxPerTxn: 50, wantTxns: 3}, // three full batches
		{items: 10, maxPerTxn: 1, wantTxns: 10},  // one transaction per item
	}

	for _, tc := range cases {
		counter := &txnCountingKV{KV: client.KV}

		items := make([]int, tc.items)
		for i := range items {
			items[i] = i
		}

		err := RunWriteTxnsInBatches(ctx, counter, items, tc.maxPerTxn, func(txn *TxnOp[NoResult], item int) {
			txn.Then(etcdop.Key(fmt.Sprintf("batch-test/%d-%d/%d", tc.items, tc.maxPerTxn, item)).Put(counter, "v"))
		})
		require.NoError(t, err, "items=%d maxPerTxn=%d", tc.items, tc.maxPerTxn)

		assert.Equal(t, tc.wantTxns, counter.count.Load(), "items=%d maxPerTxn=%d", tc.items, tc.maxPerTxn)
	}
}
