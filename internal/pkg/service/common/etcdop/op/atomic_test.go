package op_test

import (
	"context"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

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

	// Create atomic update operation
	var beforeUpdate func() (clear bool)
	var valueFromGetPhase string
	var result string
	atomicOp := op.Atomic(client, &result)
	atomicOp.OnRead(func() {
		result = "n/a"
	})
	atomicOp.OnReadOrErr(func() error {
		return nil
	})
	atomicOp.ReadOp(nil)
	atomicOp.ReadOp(key1.Get(client).WithOnResult(func(kv *op.KeyValue) {
		valueFromGetPhase = string(kv.Value)
	}))
	atomicOp.Read(func() op.Op {
		return op.MergeToTxn(
			client,
			key1.Get(client),
			key2.Delete(client),
			key3.Put(client, "value"),
			op.NewTxnOp(client).
				If(etcd.Compare(etcd.Value("key4"), "=", "value")).
				Then(
					key5.Get(client),
					op.NewTxnOp(client).
						If(etcd.Compare(etcd.Value("checkMissing"), "=", "value")).
						Then().
						Else(key8.Get(client)),
				).
				Else(key7.Get(client)),
		)
	})
	atomicOp.BeforeWriteOrErr(func() error {
		if beforeUpdate != nil {
			if clear := beforeUpdate(); clear {
				beforeUpdate = nil
			}
		}
		return nil
	})
	atomicOp.Write(func() op.Op {
		// Use a value from the GET phase in the UPDATE phase
		return key1.Put(client, "<"+valueFromGetPhase+">")
	})
	atomicOp.WriteOp(nil)
	atomicOp.WriteOp(key8.Put(client, "value").WithOnResult(func(_ op.NoResult) {
		result = "ok"
	}))

	// 1. No modification during update, DoWithoutRetry, success
	ok, err := atomicOp.DoWithoutRetry(ctx)
	assert.True(t, ok)
	require.NoError(t, err)
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
	ok, err = atomicOp.DoWithoutRetry(ctx)
	assert.False(t, ok)
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
}
