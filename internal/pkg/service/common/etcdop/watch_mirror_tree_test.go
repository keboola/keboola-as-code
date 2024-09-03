package etcdop

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

type testUser struct {
	FirstName string
	LastName  string
	Age       int
}

type testUserFullName string

func TestMirrorTree(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	// Create a typed prefix with some keys
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	pfx := NewTypedPrefix[testUser]("my/prefix", serde.NewJSON(serde.NoValidation))
	require.NoError(t, pfx.Key("key1").Put(client, testUser{FirstName: "John", LastName: "Brown", Age: 10}).Do(ctx).Err())
	require.NoError(t, pfx.Key("key2").Put(client, testUser{FirstName: "Paul", LastName: "Green", Age: 20}).Do(ctx).Err())

	// Setup mirroring of the etcd prefix tree to the memory, with custom key and value mapping.
	// The result are in-memory KV pairs "<first name> <last name>" => <age>.
	logger := log.NewDebugLogger()
	mirror := SetupMirrorTree[testUser](
		pfx.GetAllAndWatch(ctx, client, etcd.WithPrevKV()),
		func(key string, value testUser) string { return value.FirstName + " " + value.LastName },
		func(key string, value testUser, rawValue *op.KeyValue, oldValue *int) int { return value.Age },
	).
		WithFilter(func(event WatchEvent[testUser]) bool {
			return !strings.Contains(event.Kv.String(), "/ignore")
		}).
		BuildMirror()
	errCh := mirror.StartMirroring(ctx, wg, logger)

	// waitForSync:  it waits until the memory mirror is synchronized with the revision of the last change
	var header *op.Header
	waitForSync := func() {
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.GreaterOrEqual(c, mirror.Revision(), header.Revision)
		}, time.Second, 100*time.Millisecond)
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

	// WaitForRevision - in the past
	assert.NoError(t, mirror.WaitForRevision(ctx, header.Revision-1))
	assert.NoError(t, mirror.WaitForRevision(ctx, header.Revision))

	// WaitForRevision - in the future
	revInFuture := header.Revision + 1
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, mirror.WaitForRevision(ctx, revInFuture))
		assert.Equal(t, map[string]int{
			"Jacob Brown": 16, // <<<<<<<<
			"Luke Blue":   30,
		}, mirror.ToMap())
	}()
	time.Sleep(50 * time.Millisecond)
	header, err = pfx.Key("key1").Put(client, testUser{FirstName: "Jacob", LastName: "Brown", Age: 16}).Do(ctx).HeaderOrErr()
	require.NoError(t, err)
}

func TestMirrorTree_WithOnUpdate(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	// Create a typed prefix with some keys
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	pfx := NewTypedPrefix[testUser]("my/prefix", serde.NewJSON(serde.NoValidation))
	require.NoError(t, pfx.Key("key1").Put(client, testUser{FirstName: "John", LastName: "Brown", Age: 10}).Do(ctx).Err())
	require.NoError(t, pfx.Key("key2").Put(client, testUser{FirstName: "Paul", LastName: "Green", Age: 20}).Do(ctx).Err())

	// Create a channel for onUpdate results
	updateCh := make(chan MirrorUpdate)
	waitForUpdate := func() MirrorUpdate {
		select {
		case v := <-updateCh:
			v.Header = nil // clear dynamic value
			return v
		case <-time.After(5 * time.Second):
			require.Fail(t, "update timeout")
			return MirrorUpdate{}
		}
	}

	// Setup mirroring of the etcd prefix tree to the memory, with custom key and value mapping.
	// The result are in-memory KV pairs "<first name> <last name>" => <age>.
	logger := log.NewDebugLogger()
	mirror := SetupMirrorTree[testUser](
		pfx.GetAllAndWatch(ctx, client, etcd.WithPrevKV()),
		func(key string, value testUser) string { return value.FirstName + " " + value.LastName },
		func(key string, value testUser, rawValue *op.KeyValue, oldValue *int) int { return value.Age },
	).
		WithFilter(func(event WatchEvent[testUser]) bool {
			return !strings.Contains(event.Kv.String(), "/ignore")
		}).
		WithOnUpdate(func(update MirrorUpdate) {
			updateCh <- update
		}).
		BuildMirror()
	errCh := mirror.StartMirroring(ctx, wg, logger)

	// waitForSync:  it waits until the memory mirror is synchronized with the revision of the last change
	var header *op.Header
	waitForSync := func() {
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.GreaterOrEqual(c, mirror.Revision(), header.Revision)
		}, time.Second, 100*time.Millisecond)
	}

	// Wait for initialization
	require.NoError(t, <-errCh)

	// Test state after initialization
	assert.False(t, waitForUpdate().Restart)

	// Insert
	header, err := pfx.Key("key3").Put(client, testUser{FirstName: "Luke", LastName: "Blue", Age: 30}).Do(ctx).HeaderOrErr()
	require.NoError(t, err)
	waitForSync()
	assert.False(t, waitForUpdate().Restart)

	// Update
	header, err = pfx.Key("key1").Put(client, testUser{FirstName: "Jacob", LastName: "Brown", Age: 15}).Do(ctx).HeaderOrErr()
	require.NoError(t, err)
	waitForSync()
	assert.False(t, waitForUpdate().Restart)

	// Delete
	header, err = pfx.Key("key2").Delete(client).Do(ctx).HeaderOrErr()
	require.NoError(t, err)
	waitForSync()
	assert.False(t, waitForUpdate().Restart)

	// Filter
	header, err = pfx.Key("ignore").Put(client, testUser{FirstName: "Ignored", LastName: "User", Age: 50}).Do(ctx).HeaderOrErr()
	require.NoError(t, err)
	waitForSync()
	assert.Equal(t, MirrorUpdate{}, waitForUpdate())

	// Restart
	mirror.Restart(errors.New("manual restart"))
	waitForSync()
	assert.True(t, waitForUpdate().Restart)
}

func TestMirrorTree_WithOnChanges(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	// Create a typed prefix with some keys
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	pfx := NewTypedPrefix[testUser]("my/prefix", serde.NewJSON(serde.NoValidation))
	require.NoError(t, pfx.Key("key1").Put(client, testUser{FirstName: "John", LastName: "Brown", Age: 10}).Do(ctx).Err())
	require.NoError(t, pfx.Key("key2").Put(client, testUser{FirstName: "Paul", LastName: "Green", Age: 20}).Do(ctx).Err())

	// Create a channel for onChanges results
	changesCh := make(chan MirrorUpdateChanges[string, int])
	waitForUpdate := func() MirrorUpdateChanges[string, int] {
		select {
		case v := <-changesCh:
			v.Header = nil // clear dynamic value
			return v
		case <-time.After(5 * time.Second):
			require.Fail(t, "update timeout")
			return MirrorUpdateChanges[string, int]{}
		}
	}

	// Setup mirroring of the etcd prefix tree to the memory, with custom key and value mapping.
	// The result are in-memory KV pairs "<first name> <last name>" => <age>.
	logger := log.NewDebugLogger()
	mirror := SetupMirrorTree[testUser](
		pfx.GetAllAndWatch(ctx, client, etcd.WithPrevKV()),
		func(key string, value testUser) string { return value.FirstName + " " + value.LastName },
		func(key string, value testUser, rawValue *op.KeyValue, oldValue *int) int { return value.Age },
	).
		WithFilter(func(event WatchEvent[testUser]) bool {
			return !strings.Contains(event.Kv.String(), "/ignore")
		}).
		WithOnChanges(func(changes MirrorUpdateChanges[string, int]) {
			changesCh <- changes
		}).
		BuildMirror()
	errCh := mirror.StartMirroring(ctx, wg, logger)

	// waitForSync:  it waits until the memory mirror is synchronized with the revision of the last change
	var header *op.Header
	waitForSync := func() {
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.GreaterOrEqual(c, mirror.Revision(), header.Revision)
		}, time.Second, 100*time.Millisecond)
	}

	// Wait for initialization
	require.NoError(t, <-errCh)

	// Test state after initialization
	assert.Equal(t, MirrorUpdateChanges[string, int]{
		Created: []MirrorKVPair[string, int]{
			{
				Key:   "John Brown",
				Value: 10,
			},
			{
				Key:   "Paul Green",
				Value: 20,
			},
		},
	}, waitForUpdate())

	// Insert
	header, err := pfx.Key("key3").Put(client, testUser{FirstName: "Luke", LastName: "Blue", Age: 30}).Do(ctx).HeaderOrErr()
	require.NoError(t, err)
	waitForSync()
	assert.Equal(t, MirrorUpdateChanges[string, int]{
		Created: []MirrorKVPair[string, int]{
			{
				Key:   "Luke Blue",
				Value: 30,
			},
		},
	}, waitForUpdate())

	// Update
	header, err = pfx.Key("key1").Put(client, testUser{FirstName: "Jacob", LastName: "Brown", Age: 15}).Do(ctx).HeaderOrErr()
	require.NoError(t, err)
	waitForSync()
	assert.Equal(t, MirrorUpdateChanges[string, int]{
		Updated: []MirrorKVPair[string, int]{
			{
				Key:   "Jacob Brown",
				Value: 15,
			},
		},
	}, waitForUpdate())

	// Delete
	header, err = pfx.Key("key2").Delete(client).Do(ctx).HeaderOrErr()
	require.NoError(t, err)
	waitForSync()
	assert.Equal(t, MirrorUpdateChanges[string, int]{
		Deleted: []MirrorKVPair[string, int]{
			{
				Key:   "Paul Green",
				Value: 20,
			},
		},
	}, waitForUpdate())

	// Filter
	header, err = pfx.Key("ignore").Put(client, testUser{FirstName: "Ignored", LastName: "User", Age: 50}).Do(ctx).HeaderOrErr()
	require.NoError(t, err)
	waitForSync()
	assert.Equal(t, MirrorUpdateChanges[string, int]{}, waitForUpdate())

	// Restart
	mirror.Restart(errors.New("manual restart"))
	waitForSync()
	assert.Equal(t, MirrorUpdateChanges[string, int]{
		MirrorUpdate: MirrorUpdate{
			Restart: true,
		},
		Created: []MirrorKVPair[string, int]{
			{
				Key:   "Jacob Brown",
				Value: 15,
			},
			{
				Key:   "Luke Blue",
				Value: 30,
			},
		},
	}, waitForUpdate())
}

func TestFullMirrorTree(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	// Create a typed prefix with some keys
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	pfx := NewTypedPrefix[testUser]("my/prefix", serde.NewJSON(serde.NoValidation))
	require.NoError(t, pfx.Key("key1").Put(client, testUser{FirstName: "John", LastName: "Brown", Age: 10}).Do(ctx).Err())
	require.NoError(t, pfx.Key("key2").Put(client, testUser{FirstName: "Paul", LastName: "Green", Age: 20}).Do(ctx).Err())

	// Setup full mirroring of the etcd prefix tree to the memory.
	logger := log.NewDebugLogger()
	mirror := SetupFullMirrorTree(
		pfx.GetAllAndWatch(ctx, client, etcd.WithPrevKV())).
		WithFilter(func(event WatchEvent[testUser]) bool {
			return !strings.Contains(event.Kv.String(), "/ignore")
		}).
		BuildMirror()
	errCh := mirror.StartMirroring(ctx, wg, logger)

	// waitForSync:  it waits until the memory mirror is synchronized with the revision of the last change
	var header *op.Header
	waitForSync := func() {
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.GreaterOrEqual(c, mirror.Revision(), header.Revision)
		}, time.Second, 100*time.Millisecond)
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
