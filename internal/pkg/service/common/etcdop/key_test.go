package etcdop

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

type fooType string

func TestKeyOperations(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
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

	ctx := t.Context()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	k := typedKeyForTest()

	// GetKV - not found
	kv, err := k.GetKV(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Nil(t, kv)

	// GetOrNil - not found
	resultPtr, err := k.GetOrNil(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Nil(t, resultPtr)

	// GetOrEmpty - not found
	result, err := k.GetOrEmpty(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Empty(t, result)

	// GetOrErr - not found
	result, err = k.GetOrErr(client).Do(ctx).ResultOrErr()
	if assert.Error(t, err) {
		assert.True(t, errors.As(err, &op.EmptyResultError{}))
		assert.Empty(t, result)
	}

	// Exists - not found
	found, err := k.Exists(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.False(t, found)

	// ------
	// Put
	result, err = k.Put(client, "bar").Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, fooType("bar"), result)

	// GetKV - found
	kv, err = k.GetKV(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, fooType("bar"), kv.Value)

	// GetOrNil - found
	resultPtr, err = k.GetOrNil(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, fooType("bar"), *resultPtr)

	// GetOrEmpty - found
	result, err = k.GetOrEmpty(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, fooType("bar"), result)

	// GetOrErr - found
	result, err = k.GetOrErr(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, fooType("bar"), result)

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

	// GetKV - not found
	kv, err = k.GetKV(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Nil(t, kv)

	// Get - not found
	result, err = k.GetOrErr(client).Do(ctx).ResultOrErr()
	if assert.Error(t, err) {
		assert.True(t, errors.As(err, &op.EmptyResultError{}))
		assert.Empty(t, result)
	}

	// Exists - not found
	found, err = k.Exists(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.False(t, found)

	// ------
	// PutIfNotExists - key not found -> ok
	ok, err := k.PutIfNotExists(client, "value1").Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.True(t, ok)

	// GetKV - found - value 1
	kv, err = k.GetKV(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, fooType("value1"), kv.Value)

	// Get - found - value 1
	result, err = k.GetOrErr(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, fooType("value1"), result)

	// PutIfNotExists - key found -> not ok
	ok, err = k.PutIfNotExists(client, "value1").Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.False(t, ok)

	// GetKV - found - value 1
	kv, err = k.GetKV(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, fooType("value1"), kv.Value)

	// Get - found - value 1
	result, err = k.GetOrErr(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, fooType("value1"), result)

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

func TestKeyT_ReplacePrefix(t *testing.T) {
	t.Parallel()
	k := NewTypedKey[fooType]("original/prefix/foo/bar", serde.NewJSON(serde.NoValidation))
	assert.Equal(t, "original/prefix/foo/bar", k.Key())
	assert.Equal(t, "new/prefix/foo/bar", k.ReplacePrefix("original/prefix", "new/prefix").Key())
}

func BenchmarkKey_Exists(b *testing.B) {
	ctx := b.Context()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	k := Key("foo")
	if err := k.Put(client, "bar").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for range b.N {
		found, err := k.Exists(client).Do(ctx).ResultOrErr()
		if err != nil || !found {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkKey_Get(b *testing.B) {
	ctx := b.Context()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	k := Key("foo")
	if err := k.Put(client, "bar").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for range b.N {
		kv, err := k.Get(client).Do(ctx).ResultOrErr()
		if err != nil || kv == nil {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkKey_Delete(b *testing.B) {
	ctx := b.Context()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	k := Key("foo")
	if err := k.Put(client, "bar").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for i := range b.N {
		found, err := k.Delete(client).Do(ctx).ResultOrErr()
		if err != nil || found != (i == 0) {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkKey_Put(b *testing.B) {
	ctx := b.Context()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	k := Key("foo")

	b.StartTimer()

	for range b.N {
		err := k.Put(client, "bar").Do(ctx).Err()
		if err != nil {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkKey_PutIfNotExists(b *testing.B) {
	ctx := b.Context()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	k := Key("foo")

	b.StartTimer()

	for i := range b.N {
		ok, err := k.PutIfNotExists(client, "bar").Do(ctx).ResultOrErr()
		if err != nil || ok != (i == 0) {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkKeyT_Exists(b *testing.B) {
	ctx := b.Context()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	k := typedKeyForTest()
	if err := k.Put(client, "bar").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for range b.N {
		found, err := k.Exists(client).Do(ctx).ResultOrErr()
		if err != nil || !found {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkKeyT_GetKV(b *testing.B) {
	ctx := b.Context()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	k := typedKeyForTest()
	if err := k.Put(client, "bar").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for range b.N {
		kv, err := k.GetKV(client).Do(ctx).ResultOrErr()
		if err != nil || kv == nil {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkKeyT_Delete(b *testing.B) {
	ctx := b.Context()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	k := typedKeyForTest()
	if err := k.Put(client, "bar").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for i := range b.N {
		found, err := k.Delete(client).Do(ctx).ResultOrErr()
		if err != nil || found != (i == 0) {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkKeyT_Put(b *testing.B) {
	ctx := b.Context()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	k := typedKeyForTest()

	b.StartTimer()

	for range b.N {
		err := k.Put(client, "bar").Do(ctx).Err()
		if err != nil {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkKeyT_PutIfNotExists(b *testing.B) {
	ctx := b.Context()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	k := typedKeyForTest()

	b.StartTimer()

	for i := range b.N {
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
