package etcdop

import (
	"context"
	"sync"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestPrefixT_Watch(t *testing.T) {
	t.Parallel()

	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	pfx := typedPrefixForTest()

	// Create watcher
	stream := pfx.Watch(ctx, client)
	ch := stream.Channel()

	// Wait for watcher created event
	assertDone(t, func() {
		resp := <-ch
		assert.True(t, resp.Created)
		assert.NoError(t, resp.InitErr)
		assert.Empty(t, resp.Events)
	}, "watcher created timeout")

	// CREATE key
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, pfx.Key("key1").Put(client, "foo").Do(ctx).Err())
	}()

	// Wait for CREATE event
	assertDone(t, func() {
		expected := WatchEventT[fooType]{}
		expected.Value = "foo"
		expected.Type = CreateEvent
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key1"),
			Value: []byte(`"foo"`),
		}
		resp := <-ch
		assert.False(t, resp.Created)
		assert.NoError(t, resp.InitErr)
		assert.Equal(t, WatchResponseE[WatchEventT[fooType]]{Events: []WatchEventT[fooType]{expected}}, clearResponseT(resp))
	}, "CREATE timeout")

	// UPDATE key
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, pfx.Key("key1").Put(client, "new").Do(ctx).Err())
	}()

	// Wait for UPDATE event
	assertDone(t, func() {
		expected := WatchEventT[fooType]{}
		expected.Value = "new"
		expected.Type = UpdateEvent
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key1"),
			Value: []byte(`"new"`),
		}
		resp := <-ch
		assert.False(t, resp.Created)
		assert.NoError(t, resp.InitErr)
		assert.Equal(t, WatchResponseE[WatchEventT[fooType]]{Events: []WatchEventT[fooType]{expected}}, clearResponseT(resp))
	}, "UPDATE timeout")

	// DELETE key
	wg.Add(1)
	go func() {
		defer wg.Done()
		ok, err := pfx.Key("key1").Delete(client).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.True(t, ok)
	}()

	// Wait for DELETE event
	assertDone(t, func() {
		expected := WatchEventT[fooType]{}
		expected.Type = DeleteEvent
		expected.Kv = &mvccpb.KeyValue{
			Key: []byte("my/prefix/key1"),
		}
		resp := <-ch
		assert.False(t, resp.Created)
		assert.NoError(t, resp.InitErr)
		assert.Equal(t, WatchResponseE[WatchEventT[fooType]]{Events: []WatchEventT[fooType]{expected}}, clearResponseT(resp))
	}, "DELETE timeout")

	// Wait for all goroutines
	wg.Wait()

	// Channel should be closed by the context
	cancel()
	resp, ok := <-ch
	assert.False(t, ok, spew.Sdump(resp))
}

func TestPrefixT_GetAllAndWatch(t *testing.T) {
	t.Parallel()

	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	pfx := typedPrefixForTest()

	// CREATE key1
	assert.NoError(t, pfx.Key("key1").Put(client, "foo1").Do(ctx).Err())

	// Create watcher
	stream := pfx.GetAllAndWatch(ctx, client, etcd.WithPrevKV())
	ch := stream.Channel()

	// Wait for CREATE key1 event
	assertDone(t, func() {
		expected := WatchEventT[fooType]{}
		expected.Value = "foo1"
		expected.Type = CreateEvent
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key1"),
			Value: []byte(`"foo1"`),
		}
		resp := <-ch
		assert.False(t, resp.Created)
		assert.NoError(t, resp.InitErr)
		assert.Equal(t, WatchResponseE[WatchEventT[fooType]]{Events: []WatchEventT[fooType]{expected}}, clearResponseT(resp))
	}, "CREATE1 timeout")

	// Wait for watcher created event
	assertDone(t, func() {
		resp := <-ch
		assert.True(t, resp.Created)
		assert.NoError(t, resp.InitErr)
		assert.Empty(t, resp.Events)
	}, "watcher created timeout")

	// CREATE key2
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, pfx.Key("key2").Put(client, "foo2").Do(ctx).Err())
	}()

	// Wait for CREATE key1 event
	assertDone(t, func() {
		expected := WatchEventT[fooType]{}
		expected.Value = "foo2"
		expected.Type = CreateEvent
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key2"),
			Value: []byte(`"foo2"`),
		}
		resp := <-ch
		assert.False(t, resp.Created)
		assert.NoError(t, resp.InitErr)
		assert.Equal(t, WatchResponseE[WatchEventT[fooType]]{Events: []WatchEventT[fooType]{expected}}, clearResponseT(resp))
	}, "CREATE2 timeout")

	// UPDATE key
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, pfx.Key("key2").Put(client, "new").Do(ctx).Err())
	}()

	// Wait for UPDATE event
	assertDone(t, func() {
		newValue := fooType("new")
		oldValue := fooType("foo2")
		expected := WatchEventT[fooType]{}
		expected.Value = newValue
		expected.PrevValue = &oldValue
		expected.Type = UpdateEvent
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key2"),
			Value: []byte(`"new"`),
		}
		expected.PrevKv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key2"),
			Value: []byte(`"foo2"`),
		}
		resp := <-ch
		assert.False(t, resp.Created)
		assert.NoError(t, resp.InitErr)
		assert.Equal(t, WatchResponseE[WatchEventT[fooType]]{Events: []WatchEventT[fooType]{expected}}, clearResponseT(resp))
	}, "UPDATE timeout")

	// DELETE key
	wg.Add(1)
	go func() {
		defer wg.Done()
		ok, err := pfx.Key("key1").Delete(client).Do(ctx).ResultOrErr()
		assert.NoError(t, err)
		assert.True(t, ok)
	}()

	// Wait for DELETE event
	assertDone(t, func() {
		expected := WatchEventT[fooType]{}
		expected.Value = "foo1"
		expected.Type = DeleteEvent
		expected.Kv = &mvccpb.KeyValue{
			Key: []byte("my/prefix/key1"),
		}
		expected.PrevKv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key1"),
			Value: []byte(`"foo1"`),
		}
		resp := <-ch
		assert.False(t, resp.Created)
		assert.NoError(t, resp.InitErr)
		assert.Equal(t, WatchResponseE[WatchEventT[fooType]]{Events: []WatchEventT[fooType]{expected}}, clearResponseT(resp))
	}, "DELETE timeout")

	// Wait for all goroutines
	wg.Wait()

	// Channel should be closed by the context
	cancel()
	resp, ok := <-ch
	assert.False(t, ok, spew.Sdump(resp))
}

func clearResponseT(resp WatchResponseE[WatchEventT[fooType]]) WatchResponseE[WatchEventT[fooType]] {
	for i := range resp.Events {
		event := &resp.Events[i]
		event.Kv.CreateRevision = 0
		event.Kv.ModRevision = 0
		event.Kv.Version = 0
		event.Kv.Lease = 0
		if event.PrevKv != nil {
			event.PrevKv.CreateRevision = 0
			event.PrevKv.ModRevision = 0
			event.PrevKv.Version = 0
			event.PrevKv.Lease = 0
		}
	}
	resp.Header = nil
	return resp
}
