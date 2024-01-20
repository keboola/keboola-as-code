package op_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

type atomicOpTestCase struct {
	Name                string
	SkipPrefixKeysCheck bool
	Prepare             func(t *testing.T, client etcd.KV) []op.Op
	ReadPhase           func(t *testing.T, client etcd.KV) []op.Op
	BreakingChange      func(t *testing.T, client etcd.KV) []op.Op
	ExpectedWritePhase  string
	ExpectedlyDontWork  bool
}

func TestAtomicOp(t *testing.T) {
	t.Parallel()

	nop := func(_ *testing.T, client etcd.KV) []op.Op {
		return nil
	}
	getKey := func(_ *testing.T, client etcd.KV) []op.Op {
		return []op.Op{etcdop.Key("key/1").Get(client)}
	}
	getPrefix := func(_ *testing.T, client etcd.KV) []op.Op {
		return []op.Op{etcdop.Prefix("key").GetAll(client).ForEach(func(value *op.KeyValue, header *iterator.Header) error {
			return nil
		})}
	}
	putKey := func(_ *testing.T, client etcd.KV) []op.Op {
		return []op.Op{etcdop.Key("key/1").Put(client, "value")}
	}
	putTwoKeys := func(_ *testing.T, client etcd.KV) []op.Op {
		return []op.Op{
			etcdop.Key("key/1").Put(client, "value"),
			etcdop.Key("key/2").Put(client, "value"),
		}
	}
	deleteKey := func(_ *testing.T, client etcd.KV) []op.Op {
		return []op.Op{etcdop.Key("key/1").Delete(client)}
	}

	cases := []atomicOpTestCase{
		{
			// Read Phase gets a key, it doesn't exist, but before the Write Phase, it is created.
			Name:           "GetKey_CreateKey",
			Prepare:        nop,
			ReadPhase:      getKey,
			BreakingChange: putKey,
			ExpectedWritePhase: `
➡️  TXN
  ➡️  IF:
  001 "key/1" MOD EQUAL 0
  ➡️  THEN:
  001 ➡️  PUT "foo"

✔️  TXN | succeeded: false
`,
		},
		{
			// Read Phase gets a key, it exists, but before the Write Phase, it is modified.
			Name:           "GetKey_ModifyKey",
			Prepare:        putKey,
			ReadPhase:      getKey,
			BreakingChange: putKey,
			ExpectedWritePhase: `
➡️  TXN
  ➡️  IF:
  001 "key/1" MOD GREATER 0
  002 "key/1" MOD LESS %d
  ➡️  THEN:
  001 ➡️  PUT "foo"

✔️  TXN | succeeded: false
`,
		},
		{
			// Read Phase gets a key, it exists, but before the Write Phase, it is deleted.
			Name:           "GetKey_DeleteKey",
			Prepare:        putKey,
			ReadPhase:      getKey,
			BreakingChange: deleteKey,
			ExpectedWritePhase: `
➡️  TXN
  ➡️  IF:
  001 "key/1" MOD GREATER 0
  002 "key/1" MOD LESS %d
  ➡️  THEN:
  001 ➡️  PUT "foo"

✔️  TXN | succeeded: false
`,
		},
		{
			// Read Phase gets a range, but before the Write Phase, a new key is created in the range.
			Name:           "GetPrefix_CreateKey",
			Prepare:        nop,
			ReadPhase:      getPrefix,
			BreakingChange: putKey,
			ExpectedWritePhase: `
➡️  TXN
  ➡️  IF:
  001 ["key/", "key0") MOD EQUAL 0
  ➡️  THEN:
  001 ➡️  PUT "foo"

✔️  TXN | succeeded: false
`,
		},
		{
			// Read Phase gets a range, but before the Write Phase, an existing key is modified in the range.
			Name:           "GetPrefix_ModifyKey",
			Prepare:        putTwoKeys,
			ReadPhase:      getPrefix,
			BreakingChange: putKey,
			ExpectedWritePhase: `
➡️  TXN
  ➡️  IF:
  001 ["key/", "key0") MOD GREATER 0
  002 ["key/", "key0") MOD LESS %d
  003 "key/1" MOD GREATER 0
  004 "key/2" MOD GREATER 0
  ➡️  THEN:
  001 ➡️  PUT "foo"

✔️  TXN | succeeded: false
`,
		},
		{
			// Read Phase gets a range, but before the Write Phase, an existing key is deleted in the range.
			Name:           "GetPrefix_DeleteKey",
			Prepare:        putTwoKeys,
			ReadPhase:      getPrefix,
			BreakingChange: deleteKey,
			ExpectedWritePhase: `
➡️  TXN
  ➡️  IF:
  001 ["key/", "key0") MOD GREATER 0
  002 ["key/", "key0") MOD LESS %d
  003 "key/1" MOD GREATER 0
  004 "key/2" MOD GREATER 0
  ➡️  THEN:
  001 ➡️  PUT "foo"

✔️  TXN | succeeded: false
`,
		},
		{
			// Read Phase gets a range, but before the Write Phase, an existing key is deleted in the range.
			// SkipPrefixKeysCheck
			Name:                "GetPrefix_DeleteKey_SkipPrefixKeysCheck",
			SkipPrefixKeysCheck: true,
			Prepare:             putTwoKeys,
			ReadPhase:           getPrefix,
			BreakingChange:      deleteKey,
			ExpectedlyDontWork:  true,
			ExpectedWritePhase: `
➡️  TXN
  ➡️  IF:
  001 ["key/", "key0") MOD GREATER 0
  002 ["key/", "key0") MOD LESS %d
  ➡️  THEN:
  001 ➡️  PUT "foo"

✔️  TXN | succeeded: true
`,
		},
		{
			// Read Phase modifies a key, but before the Write Phase, it is modified.
			Name:           "PutKey_ModifyKey",
			Prepare:        nop,
			ReadPhase:      putKey,
			BreakingChange: putKey,
			ExpectedWritePhase: `
➡️  TXN
  ➡️  IF:
  001 "key/1" MOD GREATER 0
  002 "key/1" MOD LESS %d
  ➡️  THEN:
  001 ➡️  PUT "foo"

✔️  TXN | succeeded: false
`,
		},
		{
			// Read Phase modifies a key, but before the Write Phase, it is deleted.
			Name:           "PutKey_DeleteKey",
			Prepare:        nop,
			ReadPhase:      putKey,
			BreakingChange: deleteKey,
			ExpectedWritePhase: `
➡️  TXN
  ➡️  IF:
  001 "key/1" MOD GREATER 0
  002 "key/1" MOD LESS %d
  ➡️  THEN:
  001 ➡️  PUT "foo"

✔️  TXN | succeeded: false
`,
		},
		{
			// Read Phase deletes a key, it doesn't exist, but before the Write Phase, it is created.
			Name:           "DeleteKey_CreateKey",
			Prepare:        nop,
			ReadPhase:      deleteKey,
			BreakingChange: putKey,
			ExpectedWritePhase: `
➡️  TXN
  ➡️  IF:
  001 "key/1" MOD EQUAL 0
  ➡️  THEN:
  001 ➡️  PUT "foo"

✔️  TXN | succeeded: false
`,
		},
		{
			// Read Phase deletes a key, it exists, but before the Write Phase, it is re-created.
			Name:           "DeleteKey_RecreateKey",
			Prepare:        putKey,
			ReadPhase:      deleteKey,
			BreakingChange: putKey,
			ExpectedWritePhase: `
➡️  TXN
  ➡️  IF:
  001 "key/1" MOD EQUAL 0
  ➡️  THEN:
  001 ➡️  PUT "foo"

✔️  TXN | succeeded: false
`,
		},
	}

	for _, tc := range cases {
		tc.Run(t)
	}
}

func (tc *atomicOpTestCase) Run(t *testing.T) {
	t.Helper()

	// Check successful atomic operation - without breaking change
	t.Run(tc.Name+"-succeeded-true", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client, _ := tc.createClient(t)

		// Prepare
		if txn := op.MergeToTxn(client, tc.Prepare(t, client)...); !txn.Empty() {
			require.NoError(t, txn.Do(ctx).Err())
		}

		// Run AtomicOp
		atomicOp := op.
			Atomic(client, &op.NoResult{}).
			ReadOp(tc.ReadPhase(t, client)...).
			WriteOp(etcdop.Key("foo").Put(client, "bar"))

		if tc.SkipPrefixKeysCheck {
			atomicOp.SkipPrefixKeysCheck()
		}

		result := atomicOp.DoWithoutRetry(ctx)
		require.NoError(t, result.Err())
		assert.True(t, result.Succeeded())
	})

	// Check successful atomic operation - with breaking change
	t.Run(tc.Name+"-succeeded-false", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		client, logs := tc.createClient(t)

		// Prepare
		if txn := op.MergeToTxn(client, tc.Prepare(t, client)...); !txn.Empty() {
			require.NoError(t, txn.Do(ctx).Err())
		}

		// Run AtomicOp
		atomicOp := op.
			Atomic(client, &op.NoResult{}).
			ReadOp(tc.ReadPhase(t, client)...).
			BeforeWrite(func(ctx context.Context) {
				// Modify a key loaded by the Read Phase
				require.NoError(t, op.MergeToTxn(client, tc.BreakingChange(t, client)...).Do(ctx).Err())
				logs.Reset()
			}).
			WriteOp(etcdop.Key("foo").Put(client, "bar"))

		if tc.SkipPrefixKeysCheck {
			atomicOp.SkipPrefixKeysCheck()
		}

		result := atomicOp.DoWithoutRetry(ctx)
		require.NoError(t, result.Err())

		if tc.ExpectedlyDontWork {
			assert.True(t, result.Succeeded())
		} else {
			assert.False(t, result.Succeeded())
		}

		// Check logs
		wildcards.Assert(t, tc.ExpectedWritePhase, logs.String())
	})
}

func (tc *atomicOpTestCase) createClient(t *testing.T) (etcd.KV, *bytes.Buffer) {
	t.Helper()
	var logs bytes.Buffer
	client := etcdlogger.KVLogWrapper(
		etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t)),
		&logs,
		etcdlogger.WithMinimal(),
		etcdlogger.WithoutPutValue(),
	)
	return client, &logs
}

// TestAtomicUpdate has been partially replaced with the TestAtomicOp.
// In the future we should remove the test,
// it is necessary to make a separate test for nested transactions and for shortcuts such as the BeforeWriteOrErr method.
func TestAtomicUpdate(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Create keys
	key1 := etcdop.Key("key1")
	key2 := etcdop.Key("key2")
	key3 := etcdop.Key("key3")
	key4 := etcdop.Key("key4")
	key5 := etcdop.Key("key5")
	key6 := etcdop.Key("key6")
	key7 := etcdop.Key("key7")
	key8 := etcdop.Key("key8")
	require.NoError(t, key1.Put(client, "value").Do(ctx).Err())
	require.NoError(t, key2.Put(client, "value").Do(ctx).Err())
	require.NoError(t, key3.Put(client, "value").Do(ctx).Err())
	require.NoError(t, key4.Put(client, "value").Do(ctx).Err())
	require.NoError(t, key5.Put(client, "value").Do(ctx).Err())
	require.NoError(t, key6.Put(client, "value").Do(ctx).Err())
	require.NoError(t, key7.Put(client, "value").Do(ctx).Err())
	require.NoError(t, key8.Put(client, "value").Do(ctx).Err())

	// Create logger for processor callback
	var logger strings.Builder

	// Create atomic update operation
	var beforeUpdate func() (clear bool)
	var valueFromGetPhase string
	var result string
	atomicOp := op.Atomic(client, &result)
	atomicOp.OnRead(func(context.Context) {
		result = "n/a"
	})
	atomicOp.OnReadOrErr(func(context.Context) error {
		return nil
	})
	atomicOp.ReadOp(nil)
	atomicOp.ReadOp(key1.Get(client).WithOnResult(func(kv *op.KeyValue) {
		valueFromGetPhase = string(kv.Value)
	}))
	atomicOp.Read(func(context.Context) op.Op {
		return op.MergeToTxn(
			client,
			key1.Get(client),
			key2.Delete(client),
			key3.Put(client, "value"),
			op.Txn(client).
				If(etcd.Compare(etcd.Value("key4"), "=", "value")).
				Merge(
					key5.Get(client),
					op.Txn(client).
						If(etcd.Compare(etcd.Version("missing"), "=", 0)).
						Then().
						Else(key8.Get(client)),
				).
				Else(key7.Get(client)),
		)
	})
	atomicOp.BeforeWriteOrErr(func(context.Context) error {
		if beforeUpdate != nil {
			if clearCallback := beforeUpdate(); clearCallback {
				beforeUpdate = nil
			}
		}
		return nil
	})
	atomicOp.Write(func(context.Context) op.Op {
		// Use a value from the GET phase in the UPDATE phase
		return key1.Put(client, "<"+valueFromGetPhase+">")
	})
	atomicOp.WriteOp(nil)
	atomicOp.WriteOp(key8.Put(client, "value").WithOnResult(func(_ op.NoResult) {
		result = "ok"
	}))
	atomicOp.AddProcessor(func(ctx context.Context, result *op.Result[string]) {
		if err := result.Err(); err == nil {
			_, _ = logger.WriteString(fmt.Sprintf("atomic operation succeeded: %s\n", result.Result()))
		} else {
			_, _ = logger.WriteString(fmt.Sprintf("atomic operation failed: %s\n", err))
		}
	})

	// 1. No modification during update, DoWithoutRetry, success
	opResult := atomicOp.DoWithoutRetry(ctx)
	assert.True(t, opResult.Succeeded())
	require.NoError(t, opResult.Err())
	r, err := key1.Get(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, "<value>", string(r.Value))

	// 2. No modification during update, Do, success
	err = atomicOp.Do(ctx).Err()
	require.NoError(t, err)
	r, err = key1.Get(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, "<<value>>", string(r.Value))

	// 3. Modification during update, DoWithoutRetry, fail
	beforeUpdate = func() (clear bool) {
		require.NoError(t, key1.Put(client, "newValue1").Do(ctx).Err())
		return true
	}
	opResult = atomicOp.DoWithoutRetry(ctx)
	assert.False(t, opResult.Succeeded())
	require.NoError(t, err)
	r, err = key1.Get(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, "newValue1", string(r.Value))

	// 4. Modification during update, Do, fail
	beforeUpdate = func() (clear bool) {
		require.NoError(t, key1.Put(client, "newValue3").Do(ctx).Err())
		return false
	}
	atomicResult := atomicOp.Do(ctx, op.WithRetryMaxElapsedTime(100*time.Millisecond))
	require.Error(t, atomicResult.Err())
	wildcards.Assert(t, "atomic update failed: revision has been modified between GET and UPDATE op, attempt %d, elapsed time %s", atomicResult.Err().Error())
	assert.Equal(t, "", atomicResult.Result()) // empty value on error
	r, err = key1.Get(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, "newValue3", string(r.Value))

	// 5. Modification during update, Do, success
	beforeUpdate = func() (clear bool) {
		require.NoError(t, key1.Put(client, "newValue2").Do(ctx).Err())
		return true
	}
	atomicResult = atomicOp.Do(ctx)
	require.NoError(t, atomicResult.Err())
	assert.Equal(t, "ok", atomicResult.Result())
	r, err = key1.Get(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, "<newValue2>", string(r.Value))

	// Check processor logs, 3x success: 1., 2., 5.
	assert.Equal(t, strings.TrimSpace(`
atomic operation succeeded: ok
atomic operation succeeded: ok
atomic operation succeeded: ok
`), strings.TrimSpace(logger.String()))
}

func TestAtomicOp_AddFrom(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Create logger for processor callback
	var logger strings.Builder

	opRoot := op.
		Atomic(client, &op.NoResult{}).
		WriteOp(etcdop.Key("key0").Put(client, "0")).
		OnResult(func(op.NoResult) {
			logger.WriteString("operation root ok\n")
		})
	op1 := op.
		Atomic(client, &op.NoResult{}).
		WriteOp(etcdop.Key("key1").Put(client, "1")).
		OnResult(func(op.NoResult) {
			logger.WriteString("operation 1 ok\n")
		})
	op2 := op.
		Atomic(client, &op.NoResult{}).
		WriteOp(etcdop.Key("key2").Put(client, "2")).
		OnResult(func(op.NoResult) {
			logger.WriteString("operation 2 ok\n")
		})
	op3 := op.
		Atomic(client, &op.NoResult{}).
		WriteOp(etcdop.Key("key3").Put(client, "3")).
		OnResult(func(op.NoResult) {
			logger.WriteString("operation 3 ok\n")
		})
	op4 := op.
		Atomic(client, &op.NoResult{}).
		WriteOp(etcdop.Key("key4").Put(client, "4"))

	// Merge atomic operations and invoke all
	require.NoError(t, opRoot.AddFrom(op1).AddFrom(op2).AddFrom(op3).AddFrom(op4).Do(ctx).Err())
	assert.Equal(t, strings.TrimSpace(`
operation 1 ok
operation 2 ok
operation 3 ok
operation root ok
`), strings.TrimSpace(logger.String()))
	etcdhelper.AssertKVsString(t, client, `
<<<<<
key0
-----
0
>>>>>

<<<<<
key1
-----
1
>>>>>

<<<<<
key2
-----
2
>>>>>

<<<<<
key3
-----
3
>>>>>

<<<<<
key4
-----
4
>>>>>
`)
}
