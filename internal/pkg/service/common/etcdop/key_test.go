package etcdop

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

type fooType string

func TestKeyOperations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	k := Key("foo")

	// Get - not found
	kv, err := k.Get(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Nil(t, kv)

	// Exists - not found
	found, err := k.Exists(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.False(t, found)

	// ------
	// Put
	require.NoError(t, k.Put(client, "bar").Do(ctx).Err())

	// Get - found
	kv, err = k.Get(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, []byte("bar"), kv.Value)

	// Exists - found
	found, err = k.Exists(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.True(t, found)

	// ------
	// Delete - found
	found, err = k.Delete(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.True(t, found)

	// Delete - not found
	found, err = k.Delete(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.False(t, found)

	// Get - not found
	kv, err = k.Get(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Nil(t, kv)

	// Exists - not found
	found, err = k.Exists(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.False(t, found)

	// ------
	// PutIfNotExists - key not found -> ok
	ok, err := k.PutIfNotExists(client, "value1").Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.True(t, ok)

	// Get - found - value 1
	kv, err = k.Get(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, []byte("value1"), kv.Value)

	// PutIfNotExists - key found -> not ok
	ok, err = k.PutIfNotExists(client, "value1").Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.False(t, ok)

	// Get - found - value 1
	kv, err = k.Get(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, []byte("value1"), kv.Value)

	// DeleteIfExists - found
	ok, err = k.DeleteIfExists(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.True(t, ok)

	// DeleteIfNotExists - not found
	ok, err = k.DeleteIfExists(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestTypedKeyOperations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	k := typedKeyForTest()

	// Get - not found
	kv, err := k.Get(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Nil(t, kv)

	// Exists - not found
	found, err := k.Exists(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.False(t, found)

	// ------
	// Put
	result, err := k.Put(client, "bar").Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, fooType("bar"), result)

	// Get - found
	kv, err = k.Get(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, fooType("bar"), kv.Value)

	// Exists - found
	found, err = k.Exists(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.True(t, found)

	// ------
	// Delete - found
	found, err = k.Delete(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.True(t, found)

	// Delete - not found
	found, err = k.Delete(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.False(t, found)

	// Get - not found
	kv, err = k.Get(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Nil(t, kv)

	// Exists - not found
	found, err = k.Exists(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.False(t, found)

	// ------
	// PutIfNotExists - key not found -> ok
	ok, err := k.PutIfNotExists(client, "value1").Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.True(t, ok)

	// Get - found - value 1
	kv, err = k.Get(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, fooType("value1"), kv.Value)

	// PutIfNotExists - key found -> not ok
	ok, err = k.PutIfNotExists(client, "value1").Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.False(t, ok)

	// Get - found - value 1
	kv, err = k.Get(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, fooType("value1"), kv.Value)

	// ------
	// DeleteIfExists - found
	ok, err = k.DeleteIfExists(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.True(t, ok)

	// DeleteIfNotExists - not found
	ok, err = k.DeleteIfExists(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.False(t, ok)
}

func BenchmarkKey_Exists(b *testing.B) {
	ctx := context.Background()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	k := Key("foo")
	if err := k.Put(client, "bar").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		found, err := k.Exists(client).Do(ctx).ResultOrErr()
		if err != nil || !found {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkKey_Get(b *testing.B) {
	ctx := context.Background()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	k := Key("foo")
	if err := k.Put(client, "bar").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		kv, err := k.Get(client).Do(ctx).ResultOrErr()
		if err != nil || kv == nil {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkKey_Delete(b *testing.B) {
	ctx := context.Background()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	k := Key("foo")
	if err := k.Put(client, "bar").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		found, err := k.Delete(client).Do(ctx).ResultOrErr()
		if err != nil || found != (i == 0) {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkKey_Put(b *testing.B) {
	ctx := context.Background()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	k := Key("foo")

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		err := k.Put(client, "bar").Do(ctx).Err()
		if err != nil {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkKey_PutIfNotExists(b *testing.B) {
	ctx := context.Background()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	k := Key("foo")

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		ok, err := k.PutIfNotExists(client, "bar").Do(ctx).ResultOrErr()
		if err != nil || ok != (i == 0) {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkKeyT_Exists(b *testing.B) {
	ctx := context.Background()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	k := typedKeyForTest()
	if err := k.Put(client, "bar").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		found, err := k.Exists(client).Do(ctx).ResultOrErr()
		if err != nil || !found {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkKeyT_Get(b *testing.B) {
	ctx := context.Background()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	k := typedKeyForTest()
	if err := k.Put(client, "bar").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		kv, err := k.Get(client).Do(ctx).ResultOrErr()
		if err != nil || kv == nil {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkKeyT_Delete(b *testing.B) {
	ctx := context.Background()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	k := typedKeyForTest()
	if err := k.Put(client, "bar").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		found, err := k.Delete(client).Do(ctx).ResultOrErr()
		if err != nil || found != (i == 0) {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkKeyT_Put(b *testing.B) {
	ctx := context.Background()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	k := typedKeyForTest()

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		err := k.Put(client, "bar").Do(ctx).Err()
		if err != nil {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkKeyT_PutIfNotExists(b *testing.B) {
	ctx := context.Background()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	k := typedKeyForTest()

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		ok, err := k.PutIfNotExists(client, "bar").Do(ctx).ResultOrErr()
		if err != nil || ok != (i == 0) {
			b.Fatalf("unexpected result")
		}
	}
}

func typedKeyForTest() KeyT[fooType] {
	return KeyT[fooType]{
		key:   Key("foo"),
		serde: serde.NewJSON(serde.NoValidation),
	}
}
