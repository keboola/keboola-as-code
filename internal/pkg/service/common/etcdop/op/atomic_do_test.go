package op_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

func TestAtomicFromCtx_Misuse1(t *testing.T) {
	t.Parallel()
	assert.PanicsWithError(t, "no atomic operation found in the context", func() {
		op.AtomicOpCtxFrom(context.Background())
	})
}

func TestAtomicFromCtx_Misuse2(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(errors.New("test cancelled"))

	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	atomicOp := op.Atomic(client, &op.NoResult{}).
		Write(func(ctx context.Context) op.Op {
			return etcdop.
				NewKey("keys/key").Put(client, "value").
				WithOnResult(func(result op.NoResult) {
					op.AtomicOpCtxFrom(ctx) // <<<<<<<<< panic
				})
		})

	// The operation cannot be added to the atomic operation, in the WRITE phase
	assert.PanicsWithError(t, "atomic operation in the context is closed", func() {
		require.NoError(t, atomicOp.Do(ctx).Err())
	})
}

func TestAtomicFromCtx_Complex(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(errors.New("test cancelled"))

	// Log etcd operations
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	var etcdLogs bytes.Buffer
	client.KV = etcdlogger.KVLogWrapper(client.KV, &etcdLogs, etcdlogger.WithMinimal())

	// Create keys
	refKey := etcdop.NewKey("keys/key-ref")
	targetKey := etcdop.NewKey("keys/key123")
	require.NoError(t, refKey.Put(client, targetKey.Key()).Do(ctx).Err())
	require.NoError(t, targetKey.Put(client, "value123").Do(ctx).Err())

	// Create atomic operation
	atomicOp := op.Atomic(client, &op.NoResult{}).
		Read(func(ctx context.Context) op.Op {
			// 1. Read the reference key - value is key location
			return refKey.Get(client).WithOnResult(func(r *op.KeyValue) {
				key := string(r.Value)
				op.AtomicOpCtxFrom(ctx). // <<<<<<<<<<<<<<
					// 2. Read the key (based on the result of the previous read!)
					Read(func(ctx context.Context) op.Op {
						return etcdop.NewKey(key).Get(client).WithOnResult(func(r *op.KeyValue) {
							op.AtomicOpCtxFrom(ctx). // <<<<<<<<<<<<<<
								// 3. Write the key copy
								Write(func(ctx context.Context) op.Op {
									return etcdop.NewKey(key+"-copy").Put(client, string(r.Value))
								})
						})
					})
			})
		})

	// Do
	etcdLogs.Reset()
	require.NoError(t, atomicOp.Do(ctx).Err())
	etcdLogsStr := etcdLogs.String()

	// Check etcd state
	etcdhelper.AssertKVsString(t, client, `
<<<<<
keys/key-ref
-----
keys/key123
>>>>>

<<<<<
keys/key123
-----
value123
>>>>>

<<<<<
keys/key123-copy
-----
value123
>>>>>
`)

	// Check invoked etcd operations
	etcdlogger.Assert(t, `
// READ 1
➡️  TXN
  ➡️  THEN:
  001 ➡️  GET "keys/key-ref"
✔️  TXN | succeeded: true

// READ 2
➡️  TXN
  ➡️  THEN:
  001 ➡️  GET "keys/key123"
✔️  TXN | succeeded: true

// WRITE
➡️  TXN
  ➡️  IF:
  001 "keys/key-ref" MOD GREATER 0
  002 "keys/key-ref" MOD LESS %d
  003 "keys/key123" MOD GREATER 0
  004 "keys/key123" MOD LESS %d
  ➡️  THEN:
  001 ➡️  PUT "keys/key123-copy"
✔️  TXN | succeeded: true
`, etcdLogsStr)
}
