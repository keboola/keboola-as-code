package etcdop

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestKeyOperations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	etcd := etcdhelper.ClientForTest(t)

	k := Key("foo")

	// Get - not found
	kv, err := k.Get().Do(ctx, etcd)
	assert.NoError(t, err)
	assert.Nil(t, kv)

	// Exists - not found
	found, err := k.Exists().Do(ctx, etcd)
	assert.NoError(t, err)
	assert.False(t, found)

	// ------
	// Put
	err = k.Put("bar").Do(ctx, etcd)
	assert.NoError(t, err)

	// Get - found
	kv, err = k.Get().Do(ctx, etcd)
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, []byte("bar"), kv.Value)

	// Exists - found
	found, err = k.Exists().Do(ctx, etcd)
	assert.NoError(t, err)
	assert.True(t, found)

	// ------
	// Delete - found
	found, err = k.Delete().Do(ctx, etcd)
	assert.NoError(t, err)
	assert.True(t, found)

	// Delete - not found
	found, err = k.Delete().Do(ctx, etcd)
	assert.NoError(t, err)
	assert.False(t, found)

	// Get - not found
	kv, err = k.Get().Do(ctx, etcd)
	assert.NoError(t, err)
	assert.Nil(t, kv)

	// Exists - not found
	found, err = k.Exists().Do(ctx, etcd)
	assert.NoError(t, err)
	assert.False(t, found)

	// ------
	// PutIfNotExists - key not found -> ok
	ok, err := k.PutIfNotExists("value1").Do(ctx, etcd)
	assert.NoError(t, err)
	assert.True(t, ok)

	// Get - found - value 1
	kv, err = k.Get().Do(ctx, etcd)
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, []byte("value1"), kv.Value)

	// PutIfNotExists - key found -> not ok
	ok, err = k.PutIfNotExists("value1").Do(ctx, etcd)
	assert.NoError(t, err)
	assert.False(t, ok)

	// Get - found - value 1
	kv, err = k.Get().Do(ctx, etcd)
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	assert.Equal(t, []byte("value1"), kv.Value)
}

func BenchmarkKey_Exists(b *testing.B) {
	ctx := context.Background()
	etcd := etcdhelper.ClientForTest(b)

	k := Key("foo")
	if err := k.Put("bar").Do(ctx, etcd); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		found, err := k.Exists().Do(ctx, etcd)
		if err != nil || !found {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkKey_Get(b *testing.B) {
	ctx := context.Background()
	etcd := etcdhelper.ClientForTest(b)

	k := Key("foo")
	if err := k.Put("bar").Do(ctx, etcd); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		kv, err := k.Get().Do(ctx, etcd)
		if err != nil || kv == nil {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkKey_Delete(b *testing.B) {
	ctx := context.Background()
	etcd := etcdhelper.ClientForTest(b)

	k := Key("foo")
	if err := k.Put("bar").Do(ctx, etcd); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		found, err := k.Delete().Do(ctx, etcd)
		if err != nil || found != (i == 0) {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkKey_Put(b *testing.B) {
	ctx := context.Background()
	etcd := etcdhelper.ClientForTest(b)

	k := Key("foo")

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		err := k.Put("bar").Do(ctx, etcd)
		if err != nil {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkKey_PutIfNotExists(b *testing.B) {
	ctx := context.Background()
	etcd := etcdhelper.ClientForTest(b)

	k := Key("foo")

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		ok, err := k.PutIfNotExists("bar").Do(ctx, etcd)
		if err != nil || ok != (i == 0) {
			b.Fatalf("unexpected result")
		}
	}
}
