package op

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestTracker(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	tracker := NewTracker(client)

	// Create some keys
	_, err := client.Put(ctx, "key1", "value")
	require.NoError(t, err)
	_, err = client.Put(ctx, "key2", "value")
	require.NoError(t, err)
	_, err = client.Put(ctx, "key3", "value")
	require.NoError(t, err)
	_, err = client.Put(ctx, "key4", "value")
	require.NoError(t, err)
	_, err = client.Put(ctx, "key5", "value")
	require.NoError(t, err)
	_, err = client.Put(ctx, "key6", "value")
	require.NoError(t, err)
	_, err = client.Put(ctx, "key7", "value")
	require.NoError(t, err)

	// Test all KV operations: get, del, put, txn + get prefix, get range
	_, err = tracker.Get(ctx, "key1")
	require.NoError(t, err)
	_, err = tracker.Get(ctx, "key1")
	require.NoError(t, err)
	_, err = tracker.Delete(ctx, "key2")
	require.NoError(t, err)
	_, err = tracker.Put(ctx, "key3", "value")
	require.NoError(t, err)
	_, err = tracker.Txn(ctx).
		If(etcd.Compare(etcd.Value("key4"), "=", "value")).
		Then(
			etcd.OpGet("key5"),
			etcd.OpTxn(
				[]etcd.Cmp{
					etcd.Compare(etcd.Value("checkMissing"), "=", "value"),
				},
				[]etcd.Op{
					etcd.OpGet("key6"),
				},
				[]etcd.Op{
					etcd.OpGet("key", etcd.WithPrefix()),
					etcd.OpGet("key10", etcd.WithRange("key20")),
				},
			)).
		Else(etcd.OpGet("key7")).
		Commit()
	require.NoError(t, err)

	// Get records
	operations := tracker.Operations()
	for i := range operations {
		op := &operations[i]
		// Clear complex KVs slice, assert count
		if op.Type == GetOp {
			assert.Len(t, op.KVs, int(op.Count))
			op.KVs = nil
		}
	}

	// Check tracked records, no duplicates
	assert.Equal(t, []TrackedOp{
		{Type: GetOp, Key: []byte("key1"), Count: 1},
		{Type: DeleteOp, Key: []byte("key2"), Count: 1},
		{Type: PutOp, Key: []byte("key3"), Count: 1},
		{Type: GetOp, Key: []byte("key5"), Count: 1},
		{Type: GetOp, Key: []byte("key"), RangeEnd: []byte("kez"), Count: 6},
		{Type: GetOp, Key: []byte("key10"), RangeEnd: []byte("key20"), Count: 0},
		// all except key6 and key7, they are from unused transaction branches
	}, operations)
}
