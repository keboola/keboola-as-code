package etcdop

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestPrefix(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	root := Prefix("my")
	pfx := root.Add("prefix")
	key0 := Key("key0")
	key1 := pfx.Key("key1")
	key2 := pfx.Key("key2")

	err := key0.Put(client, "out of the prefix").Do(ctx).Err()
	require.NoError(t, err)

	// AtLeastOneExists - not found
	found, err := pfx.AtLeastOneExists(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.False(t, found)

	// Count - 0
	count, err := pfx.Count(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	// GetAll - empty
	kvs, err := pfx.GetAll(client).Do(ctx).All()
	require.NoError(t, err)
	assert.Empty(t, kvs)

	// GetOne - empty
	kv, err := pfx.GetOne(client, etcd.WithSort(etcd.SortByKey, etcd.SortDescend)).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Nil(t, kv)

	// DeleteAll - empty
	deleted, err := pfx.DeleteAll(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted)

	// ---
	require.NoError(t, key1.Put(client, "foo").Do(ctx).Err())

	// AtLeastOneExists - found 1
	found, err = pfx.AtLeastOneExists(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.True(t, found)

	// Count - 1
	count, err = pfx.Count(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// GetAll - found 1
	kvs, err = pfx.GetAll(client).Do(ctx).All()
	require.NoError(t, err)
	assert.NotEmpty(t, kvs)
	assert.Len(t, kvs, 1)
	assert.Equal(t, []byte("foo"), kvs[0].Value)

	// DeleteAll - deleted 1
	deleted, err = pfx.DeleteAll(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	// ---
	require.NoError(t, key1.Put(client, "foo").Do(ctx).Err())
	require.NoError(t, key2.Put(client, "bar").Do(ctx).Err())

	// AtLeastOneExists - found 2
	found, err = pfx.AtLeastOneExists(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.True(t, found)

	// Count - 2
	count, err = pfx.Count(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// GetAll - found 2
	kvs, err = pfx.GetAll(client).Do(ctx).All()
	require.NoError(t, err)
	assert.NotEmpty(t, kvs)
	assert.Len(t, kvs, 2)
	assert.Equal(t, []byte("foo"), kvs[0].Value)
	assert.Equal(t, []byte("bar"), kvs[1].Value)

	// GetOne - 2 exists
	kv, err = pfx.GetOne(client, etcd.WithSort(etcd.SortByKey, etcd.SortDescend)).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, []byte("my/prefix/key2"), kv.Key)
	assert.Equal(t, []byte("bar"), kv.Value)

	// DeleteAll - deleted 2
	deleted, err = pfx.DeleteAll(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted)
}

func TestTypedPrefix(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	pfx := typedPrefixForTest()
	key0 := Key("key0")
	key1 := pfx.Key("key1")
	key2 := pfx.Key("key2")

	err := key0.Put(client, "out of the prefix").Do(ctx).Err()
	require.NoError(t, err)

	// AtLeastOneExists - not found
	found, err := pfx.AtLeastOneExists(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.False(t, found)

	// Count - 0
	count, err := pfx.Count(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	// GetAll - empty
	kvs, err := pfx.GetAll(client).Do(ctx).AllKVs()
	require.NoError(t, err)
	assert.Empty(t, kvs)

	// GetOne - empty
	kv, err := pfx.GetOne(client, etcd.WithSort(etcd.SortByKey, etcd.SortDescend)).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Nil(t, kv)

	// GetOne - empty
	resultPtr, err := pfx.GetOne(client, etcd.WithSort(etcd.SortByKey, etcd.SortDescend)).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Nil(t, resultPtr)

	// DeleteAll - empty
	deleted, err := pfx.DeleteAll(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted)

	// ---
	require.NoError(t, key1.Put(client, "foo").Do(ctx).Err())

	// AtLeastOneExists - found 1
	found, err = pfx.AtLeastOneExists(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.True(t, found)

	// Count - 1
	count, err = pfx.Count(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// GetAll - found 1
	kvs, err = pfx.GetAll(client).Do(ctx).AllKVs()
	require.NoError(t, err)
	assert.NotEmpty(t, kvs)
	assert.Len(t, kvs, 1)
	assert.Equal(t, fooType("foo"), kvs[0].Value)

	// DeleteAll - deleted 1
	deleted, err = pfx.DeleteAll(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	// ---
	require.NoError(t, key1.Put(client, "foo").Do(ctx).Err())
	require.NoError(t, key2.Put(client, "bar").Do(ctx).Err())

	// AtLeastOneExists - found 2
	found, err = pfx.AtLeastOneExists(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.True(t, found)

	// Count - 2
	count, err = pfx.Count(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// GetAll - found 2
	kvs, err = pfx.GetAll(client).Do(ctx).AllKVs()
	require.NoError(t, err)
	assert.NotEmpty(t, kvs)
	assert.Len(t, kvs, 2)
	assert.Equal(t, fooType("foo"), kvs[0].Value)
	assert.Equal(t, fooType("bar"), kvs[1].Value)

	// GetOneKV - 2 exists
	kvp, err := pfx.GetOneKV(client, etcd.WithSort(etcd.SortByKey, etcd.SortDescend)).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, "my/prefix/key2", kvp.Key())
	assert.Equal(t, fooType("bar"), kvp.Value)

	// GetOne - 2 exists
	resultPtr, err = pfx.GetOne(client, etcd.WithSort(etcd.SortByKey, etcd.SortDescend)).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, fooType("bar"), *resultPtr)

	// DeleteAll - deleted 2
	deleted, err = pfx.DeleteAll(client).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted)
}

func BenchmarkPrefix_AtLestOneExists(b *testing.B) {
	ctx := b.Context()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	pfx := Prefix("my/prefix/")
	if err := Key("key0").Put(client, "foo").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}
	if err := pfx.Key("key1").Put(client, "bar").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}
	if err := pfx.Key("key2").Put(client, "baz").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for range b.N {
		found, err := pfx.AtLeastOneExists(client).Do(ctx).ResultOrErr()
		if err != nil || !found {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkPrefix_Count(b *testing.B) {
	ctx := b.Context()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	pfx := Prefix("my/prefix/")
	if err := Key("key0").Put(client, "foo").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}
	if err := pfx.Key("key1").Put(client, "bar").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}
	if err := pfx.Key("key2").Put(client, "baz").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for range b.N {
		count, err := pfx.Count(client).Do(ctx).ResultOrErr()
		if err != nil || count != int64(2) {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkPrefix_GetAll(b *testing.B) {
	ctx := b.Context()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	pfx := Prefix("my/prefix/")
	if err := Key("key0").Put(client, "foo").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}
	for i := range 100 {
		key := fmt.Sprintf("key%04d", i)
		if err := pfx.Key(key).Put(client, "bar").Do(ctx).Err(); err != nil {
			b.Fatalf(`cannot create etcd key "%s": %s`, key, err)
		}
	}

	b.StartTimer()

	for range b.N {
		kvs, err := pfx.GetAll(client).Do(ctx).All()
		if err != nil || len(kvs) != 100 {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkPrefix_DeleteAll(b *testing.B) {
	ctx := b.Context()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	pfx := Prefix("my/prefix/")
	if err := Key("key0").Put(client, "foo").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}
	if err := pfx.Key("key1").Put(client, "bar").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}
	if err := pfx.Key("key2").Put(client, "baz").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for i := range b.N {
		deleted, err := pfx.DeleteAll(client).Do(ctx).ResultOrErr()
		if err != nil || (i == 0 != (deleted == 2)) { // xor
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkPrefixT_AtLestOneExists(b *testing.B) {
	ctx := b.Context()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	pfx := typedPrefixForTest()
	if err := Key("key0").Put(client, "foo").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}
	if err := pfx.Key("key1").Put(client, "bar").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}
	if err := pfx.Key("key2").Put(client, "baz").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for range b.N {
		found, err := pfx.AtLeastOneExists(client).Do(ctx).ResultOrErr()
		if err != nil || !found {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkPrefixT_Count(b *testing.B) {
	ctx := b.Context()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	pfx := typedPrefixForTest()
	if err := Key("key0").Put(client, "foo").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}
	if err := pfx.Key("key1").Put(client, "bar").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}
	if err := pfx.Key("key2").Put(client, "baz").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for range b.N {
		count, err := pfx.Count(client).Do(ctx).ResultOrErr()
		if err != nil || count != int64(2) {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkPrefixT_GetAll(b *testing.B) {
	ctx := b.Context()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	pfx := typedPrefixForTest()
	if err := Key("key0").Put(client, "foo").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}
	for i := range 100 {
		key := fmt.Sprintf("key%04d", i)
		if err := pfx.Key(key).Put(client, "bar").Do(ctx).Err(); err != nil {
			b.Fatalf(`cannot create etcd key "%s": %s`, key, err)
		}
	}

	b.StartTimer()

	for range b.N {
		kvs, err := pfx.GetAll(client).Do(ctx).AllKVs()
		if err != nil || len(kvs) != 100 {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkPrefixT_DeleteAll(b *testing.B) {
	ctx := b.Context()
	client := etcdhelper.ClientForTest(b, etcdhelper.TmpNamespace(b))

	pfx := typedPrefixForTest()
	if err := Key("key0").Put(client, "foo").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}
	if err := pfx.Key("key1").Put(client, "bar").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}
	if err := pfx.Key("key2").Put(client, "baz").Do(ctx).Err(); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for i := range b.N {
		deleted, err := pfx.DeleteAll(client).Do(ctx).ResultOrErr()
		if err != nil || (i == 0 != (deleted == 2)) { // xor
			b.Fatalf("unexpected result")
		}
	}
}

func prefixForTest() Prefix {
	return Prefix("my").Add("prefix")
}

func typedPrefixForTest() PrefixT[fooType] {
	return PrefixT[fooType]{
		prefix: prefixForTest(),
		serde:  serde.NewJSON(serde.NoValidation),
	}
}
