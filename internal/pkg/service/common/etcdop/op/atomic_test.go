package op_test

import (
	"context"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestAtomicUpdate(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := etcdhelper.ClientForTest(t)

	// Create keys
	key1 := etcdop.Key("key1")
	key2 := etcdop.Key("key2")
	key3 := etcdop.Key("key3")
	key4 := etcdop.Key("key4")
	key5 := etcdop.Key("key5")
	key6 := etcdop.Key("key6")
	key7 := etcdop.Key("key7")
	key8 := etcdop.Key("key8")
	assert.NoError(t, key1.Put("value").Do(ctx, client))
	assert.NoError(t, key2.Put("value").Do(ctx, client))
	assert.NoError(t, key3.Put("value").Do(ctx, client))
	assert.NoError(t, key4.Put("value").Do(ctx, client))
	assert.NoError(t, key5.Put("value").Do(ctx, client))
	assert.NoError(t, key6.Put("value").Do(ctx, client))
	assert.NoError(t, key7.Put("value").Do(ctx, client))
	assert.NoError(t, key8.Put("value").Do(ctx, client))

	// Create atomic update operation
	var beforeUpdate func() (clear bool)
	var valueFromGetPhase string
	atomicOp := op.Atomic()
	atomicOp.Read(func() op.Op {
		return op.MergeToTxn(
			key1.Get(),
			key1.Get().WithOnResult(func(kv *op.KeyValue) {
				valueFromGetPhase = string(kv.Value)
			}),
			key2.Delete(),
			key3.Put("value"),
			op.NewTxnOp().
				If(etcd.Compare(etcd.Value("key4"), "=", "value")).
				Then(
					key5.Get(),
					op.NewTxnOp().
						If(etcd.Compare(etcd.Value("checkMissing"), "=", "value")).
						Then().
						Else(key8.Get()),
				).
				Else(key7.Get()),
		)
	})
	atomicOp.Write(func() op.Op {
		if beforeUpdate != nil {
			if clear := beforeUpdate(); clear {
				beforeUpdate = nil
			}
		}

		// Use a value from the GET phase in the UPDATE phase
		return key1.Put("<" + valueFromGetPhase + ">")
	})

	// 1. No modification during update, DoWithoutRetry, success
	ok, err := atomicOp.DoWithoutRetry(ctx, client)
	assert.True(t, ok)
	assert.NoError(t, err)
	r, err := key1.Get().Do(ctx, client)
	assert.NoError(t, err)
	assert.Equal(t, "<value>", string(r.Value))

	// 2. No modification during update, Do, success
	err = atomicOp.Do(ctx, client)
	assert.NoError(t, err)
	r, err = key1.Get().Do(ctx, client)
	assert.NoError(t, err)
	assert.Equal(t, "<<value>>", string(r.Value))

	// 3. Modification during update, DoWithoutRetry, fail
	beforeUpdate = func() (clear bool) {
		assert.NoError(t, key1.Put("newValue1").DoOrErr(ctx, client))
		return true
	}
	ok, err = atomicOp.DoWithoutRetry(ctx, client)
	assert.False(t, ok)
	assert.NoError(t, err)
	r, err = key1.Get().Do(ctx, client)
	assert.NoError(t, err)
	assert.Equal(t, "newValue1", string(r.Value))

	// 4. Modification during update, Do, fail
	beforeUpdate = func() (clear bool) {
		assert.NoError(t, key1.Put("newValue3").DoOrErr(ctx, client))
		return false
	}
	err = atomicOp.Do(ctx, client, op.WithRetryMaxElapsedTime(100*time.Millisecond))
	assert.Error(t, err)
	wildcards.Assert(t, "atomic update failed: revision has been modified between GET and UPDATE op, attempt %d, elapsed time %s", err.Error())
	r, err = key1.Get().Do(ctx, client)
	assert.NoError(t, err)
	assert.Equal(t, "newValue3", string(r.Value))

	// 5. Modification during update, Do, success
	beforeUpdate = func() (clear bool) {
		assert.NoError(t, key1.Put("newValue2").DoOrErr(ctx, client))
		return true
	}
	assert.NoError(t, atomicOp.Do(ctx, client))
	r, err = key1.Get().Do(ctx, client)
	assert.NoError(t, err)
	assert.Equal(t, "<newValue2>", string(r.Value))
}
