package etcdop

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

type testUser struct {
	FirstName string
	LastName  string
	Age       int
}

func TestMirror(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Create a typed prefix with some keys
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	pfx := NewTypedPrefix[testUser]("my/prefix", serde.NewJSON(serde.NoValidation))
	require.NoError(t, pfx.Key("key1").Put(client, testUser{FirstName: "John", LastName: "Brown", Age: 10}).Do(ctx).Err())
	require.NoError(t, pfx.Key("key2").Put(client, testUser{FirstName: "Paul", LastName: "Green", Age: 20}).Do(ctx).Err())

	// Setup mirroring of the etcd prefix tree to the memory, with custom key and value mapping.
	// The result are in-memory KV pairs "<first name> <last name>" => <age>.
	logger := log.NewDebugLogger()
	mirror, errCh := SetupMirror(
		logger,
		pfx.GetAllAndWatch(ctx, client, clientv3.WithPrevKV()),
		func(kv *op.KeyValue, v testUser) string { return v.FirstName + " " + v.LastName },
		func(kv *op.KeyValue, v testUser) int { return v.Age },
	).
		WithFilter(func(event WatchEventT[testUser]) bool {
			return !strings.Contains(event.Kv.String(), "/ignore")
		}).
		StartMirroring(ctx, wg)

	// waitForSync:  it waits until the memory mirror is synchronized with the revision of the last change
	var header *op.Header
	waitForSync := func() {
		assert.Eventually(t, func() bool { return mirror.Revision() >= header.Revision }, time.Second, 100*time.Millisecond)
	}

	// Wait for initialization
	require.NoError(t, <-errCh)

	// Test state after initialization
	assert.Equal(t, map[string]int{
		"John Brown": 10,
		"Paul Green": 20,
	}, mirror.ToMap())

	// Insert
	header, err := pfx.Key("key3").Put(client, testUser{FirstName: "Luke", LastName: "Blue", Age: 30}).Do(ctx).HeaderOrErr()
	require.NoError(t, err)
	waitForSync()
	assert.Equal(t, map[string]int{
		"John Brown": 10,
		"Paul Green": 20,
		"Luke Blue":  30,
	}, mirror.ToMap())

	// Update
	header, err = pfx.Key("key1").Put(client, testUser{FirstName: "Jacob", LastName: "Brown", Age: 15}).Do(ctx).HeaderOrErr()
	require.NoError(t, err)
	waitForSync()
	assert.Equal(t, map[string]int{
		"Jacob Brown": 15,
		"Paul Green":  20,
		"Luke Blue":   30,
	}, mirror.ToMap())

	// Delete
	header, err = pfx.Key("key2").Delete(client).Do(ctx).HeaderOrErr()
	require.NoError(t, err)
	waitForSync()
	assert.Equal(t, map[string]int{
		"Jacob Brown": 15,
		"Luke Blue":   30,
	}, mirror.ToMap())

	// Filter
	header, err = pfx.Key("ignore").Put(client, testUser{FirstName: "Ignored", LastName: "User", Age: 50}).Do(ctx).HeaderOrErr()
	require.NoError(t, err)
	waitForSync()
	assert.Equal(t, map[string]int{
		"Jacob Brown": 15,
		"Luke Blue":   30,
	}, mirror.ToMap())
}

func TestFullMirror(t *testing.T) {
	t.Parallel()

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Create a typed prefix with some keys
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	pfx := NewTypedPrefix[testUser]("my/prefix", serde.NewJSON(serde.NoValidation))
	require.NoError(t, pfx.Key("key1").Put(client, testUser{FirstName: "John", LastName: "Brown", Age: 10}).Do(ctx).Err())
	require.NoError(t, pfx.Key("key2").Put(client, testUser{FirstName: "Paul", LastName: "Green", Age: 20}).Do(ctx).Err())

	// Setup full mirroring of the etcd prefix tree to the memory.
	logger := log.NewDebugLogger()
	mirror, errCh := SetupFullMirror(
		logger,
		pfx.GetAllAndWatch(ctx, client, clientv3.WithPrevKV())).
		WithFilter(func(event WatchEventT[testUser]) bool {
			return !strings.Contains(event.Kv.String(), "/ignore")
		}).
		StartMirroring(ctx, wg)

	// waitForSync:  it waits until the memory mirror is synchronized with the revision of the last change
	var header *op.Header
	waitForSync := func() {
		assert.Eventually(t, func() bool { return mirror.Revision() >= header.Revision }, 5*time.Second, 100*time.Millisecond)
	}

	// Wait for initialization
	require.NoError(t, <-errCh)

	// Test state after initialization
	assert.Equal(t, map[string]testUser{
		"my/prefix/key1": {FirstName: "John", LastName: "Brown", Age: 10},
		"my/prefix/key2": {FirstName: "Paul", LastName: "Green", Age: 20},
	}, mirror.ToMap())

	// Insert
	header, err := pfx.Key("key3").Put(client, testUser{FirstName: "Luke", LastName: "Blue", Age: 30}).Do(ctx).HeaderOrErr()
	require.NoError(t, err)
	waitForSync()
	assert.Equal(t, map[string]testUser{
		"my/prefix/key1": {FirstName: "John", LastName: "Brown", Age: 10},
		"my/prefix/key2": {FirstName: "Paul", LastName: "Green", Age: 20},
		"my/prefix/key3": {FirstName: "Luke", LastName: "Blue", Age: 30},
	}, mirror.ToMap())

	// Update
	header, err = pfx.Key("key1").Put(client, testUser{FirstName: "Jacob", LastName: "Brown", Age: 15}).Do(ctx).HeaderOrErr()
	require.NoError(t, err)
	waitForSync()
	assert.Equal(t, map[string]testUser{
		"my/prefix/key1": {FirstName: "Jacob", LastName: "Brown", Age: 15},
		"my/prefix/key2": {FirstName: "Paul", LastName: "Green", Age: 20},
		"my/prefix/key3": {FirstName: "Luke", LastName: "Blue", Age: 30},
	}, mirror.ToMap())

	// Delete
	header, err = pfx.Key("key2").Delete(client).Do(ctx).HeaderOrErr()
	require.NoError(t, err)
	waitForSync()
	assert.Equal(t, map[string]testUser{
		"my/prefix/key1": {FirstName: "Jacob", LastName: "Brown", Age: 15},
		"my/prefix/key3": {FirstName: "Luke", LastName: "Blue", Age: 30},
	}, mirror.ToMap())

	// Filter
	header, err = pfx.Key("ignore").Put(client, testUser{FirstName: "Ignored", LastName: "User", Age: 50}).Do(ctx).HeaderOrErr()
	require.NoError(t, err)
	waitForSync()
	assert.Equal(t, map[string]testUser{
		"my/prefix/key1": {FirstName: "Jacob", LastName: "Brown", Age: 15},
		"my/prefix/key3": {FirstName: "Luke", LastName: "Blue", Age: 30},
	}, mirror.ToMap())
}
