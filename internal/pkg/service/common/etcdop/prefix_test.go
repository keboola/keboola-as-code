package etcdop

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestPrefix(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	etcd := etcdhelper.ClientForTest(t)

	pfx := Prefix("my/prefix/")
	key0 := Key("key0")
	key1 := Key("my/prefix/key1")
	key2 := Key("my/prefix/key2")

	err := key0.Put("out of the prefix").Do(ctx, etcd)
	assert.NoError(t, err)

	// AtLestOneExists - not found
	found, err := pfx.AtLestOneExists().Do(ctx, etcd)
	assert.NoError(t, err)
	assert.False(t, found)

	// Count - 0
	count, err := pfx.Count().Do(ctx, etcd)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)

	// GetAll - empty
	kvs, err := pfx.GetAll().Do(ctx, etcd)
	assert.NoError(t, err)
	assert.Empty(t, kvs)

	// DeleteAll - empty
	deleted, err := pfx.DeleteAll().Do(ctx, etcd)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), deleted)

	// ---
	err = key1.Put("foo").Do(ctx, etcd)
	assert.NoError(t, err)

	// AtLestOneExists - found 1
	found, err = pfx.AtLestOneExists().Do(ctx, etcd)
	assert.NoError(t, err)
	assert.True(t, found)

	// Count - 1
	count, err = pfx.Count().Do(ctx, etcd)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// GetAll - found 1
	kvs, err = pfx.GetAll().Do(ctx, etcd)
	assert.NoError(t, err)
	assert.NotEmpty(t, kvs)
	assert.Len(t, kvs, 1)
	assert.Equal(t, []byte("foo"), kvs[0].Value)

	// DeleteAll - deleted 1
	deleted, err = pfx.DeleteAll().Do(ctx, etcd)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	// ---
	err = key1.Put("foo").Do(ctx, etcd)
	assert.NoError(t, err)
	err = key2.Put("bar").Do(ctx, etcd)
	assert.NoError(t, err)

	// AtLestOneExists - found 2
	found, err = pfx.AtLestOneExists().Do(ctx, etcd)
	assert.NoError(t, err)
	assert.True(t, found)

	// Count - 2
	count, err = pfx.Count().Do(ctx, etcd)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// GetAll - found 2
	kvs, err = pfx.GetAll().Do(ctx, etcd)
	assert.NoError(t, err)
	assert.NotEmpty(t, kvs)
	assert.Len(t, kvs, 2)
	assert.Equal(t, []byte("foo"), kvs[0].Value)
	assert.Equal(t, []byte("bar"), kvs[1].Value)

	// DeleteAll - deleted 2
	deleted, err = pfx.DeleteAll().Do(ctx, etcd)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), deleted)
}

func BenchmarkPrefix_AtLestOneExists(b *testing.B) {
	ctx := context.Background()
	etcd := etcdhelper.ClientForTest(b)

	pfx := Prefix("my/prefix/")
	if err := Key("key0").Put("foo").Do(ctx, etcd); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}
	if err := Key("my/prefix/key1").Put("bar").Do(ctx, etcd); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}
	if err := Key("my/prefix/key2").Put("baz").Do(ctx, etcd); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		found, err := pfx.AtLestOneExists().Do(ctx, etcd)
		if err != nil || !found {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkPrefix_Count(b *testing.B) {
	ctx := context.Background()
	etcd := etcdhelper.ClientForTest(b)

	pfx := Prefix("my/prefix/")
	if err := Key("key0").Put("foo").Do(ctx, etcd); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}
	if err := Key("my/prefix/key1").Put("bar").Do(ctx, etcd); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}
	if err := Key("my/prefix/key2").Put("baz").Do(ctx, etcd); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		count, err := pfx.Count().Do(ctx, etcd)
		if err != nil || count != int64(2) {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkPrefix_GetAll(b *testing.B) {
	ctx := context.Background()
	etcd := etcdhelper.ClientForTest(b)

	pfx := Prefix("my/prefix/")
	if err := Key("key0").Put("foo").Do(ctx, etcd); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}
	if err := Key("my/prefix/key1").Put("bar").Do(ctx, etcd); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}
	if err := Key("my/prefix/key2").Put("baz").Do(ctx, etcd); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		kvs, err := pfx.GetAll().Do(ctx, etcd)
		if err != nil || len(kvs) != 2 {
			b.Fatalf("unexpected result")
		}
	}
}

func BenchmarkPrefix_DeleteAll(b *testing.B) {
	ctx := context.Background()
	etcd := etcdhelper.ClientForTest(b)

	pfx := Prefix("my/prefix/")
	if err := Key("key0").Put("foo").Do(ctx, etcd); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}
	if err := Key("my/prefix/key1").Put("bar").Do(ctx, etcd); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}
	if err := Key("my/prefix/key2").Put("baz").Do(ctx, etcd); err != nil {
		b.Fatalf("cannot create etcd key: %s", err)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		deleted, err := pfx.DeleteAll().Do(ctx, etcd)
		if err != nil || (i == 0 != (deleted == 2)) { // xor
			b.Fatalf("unexpected result")
		}
	}
}
