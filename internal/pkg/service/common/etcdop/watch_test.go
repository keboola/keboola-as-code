package etcdop

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/keboola/go-utils/pkg/wildcards"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestPrefix_Watch(t *testing.T) {
	t.Parallel()

	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := etcdhelper.ClientForTest(t)
	pfx := prefixForTest()

	// Create watcher
	ch := pfx.Watch(ctx, c)

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
		assert.NoError(t, pfx.Key("key1").Put("foo").Do(ctx, c))
	}()

	// Wait for CREATE event
	assertDone(t, func() {
		expected := WatchEvent{}
		expected.Type = CreateEvent
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key1"),
			Value: []byte("foo"),
		}
		resp := <-ch
		assert.False(t, resp.Created)
		assert.NoError(t, resp.InitErr)
		assert.Equal(t, WatchResponse{Events: []WatchEvent{expected}}, clearResponse(resp))
	}, "CREATE timeout")

	// UPDATE key
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, pfx.Key("key1").Put("new").Do(ctx, c))
	}()

	// Wait for UPDATE event
	assertDone(t, func() {
		expected := WatchEvent{}
		expected.Type = UpdateEvent
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key1"),
			Value: []byte("new"),
		}
		resp := <-ch
		assert.False(t, resp.Created)
		assert.NoError(t, resp.InitErr)
		assert.Equal(t, WatchResponse{Events: []WatchEvent{expected}}, clearResponse(resp))
	}, "UPDATE timeout")

	// DELETE key
	wg.Add(1)
	go func() {
		defer wg.Done()
		ok, err := pfx.Key("key1").Delete().Do(ctx, c)
		assert.NoError(t, err)
		assert.True(t, ok)
	}()

	// Wait for DELETE event
	assertDone(t, func() {
		expected := WatchEvent{}
		expected.Type = DeleteEvent
		expected.Kv = &mvccpb.KeyValue{
			Key: []byte("my/prefix/key1"),
		}
		resp := <-ch
		assert.False(t, resp.Created)
		assert.NoError(t, resp.InitErr)
		assert.Equal(t, WatchResponse{Events: []WatchEvent{expected}}, clearResponse(resp))
	}, "DELETE timeout")

	// Wait for all goroutines
	wg.Wait()

	// Channel should be closed by the context
	cancel()
	_, ok := <-ch
	assert.False(t, ok)
}

func TestPrefix_GetAllAndWatch(t *testing.T) {
	t.Parallel()

	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := etcdhelper.ClientForTest(t)
	pfx := prefixForTest()

	// CREATE key1
	assert.NoError(t, pfx.Key("key1").Put("foo1").Do(ctx, c))

	// Create watcher
	ch := pfx.GetAllAndWatch(ctx, c)

	// Wait for CREATE key1 event
	assertDone(t, func() {
		expected := WatchEvent{}
		expected.Type = CreateEvent
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key1"),
			Value: []byte("foo1"),
		}
		resp := <-ch
		assert.False(t, resp.Created)
		assert.NoError(t, resp.InitErr)
		assert.Equal(t, WatchResponse{Events: []WatchEvent{expected}}, clearResponse(resp))
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
		assert.NoError(t, pfx.Key("key2").Put("foo2").Do(ctx, c))
	}()

	// Wait for CREATE key1 event
	assertDone(t, func() {
		expected := WatchEvent{}
		expected.Type = CreateEvent
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key2"),
			Value: []byte("foo2"),
		}
		resp := <-ch
		assert.False(t, resp.Created)
		assert.NoError(t, resp.InitErr)
		assert.Equal(t, WatchResponse{Events: []WatchEvent{expected}}, clearResponse(resp))
	}, "CREATE2 timeout")

	// UPDATE key
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, pfx.Key("key2").Put("new").Do(ctx, c))
	}()

	// Wait for UPDATE event
	assertDone(t, func() {
		expected := WatchEvent{}
		expected.Type = UpdateEvent
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key2"),
			Value: []byte("new"),
		}
		resp := <-ch
		assert.False(t, resp.Created)
		assert.NoError(t, resp.InitErr)
		assert.Equal(t, WatchResponse{Events: []WatchEvent{expected}}, clearResponse(resp))
	}, "UPDATE timeout")

	// DELETE key
	wg.Add(1)
	go func() {
		defer wg.Done()
		ok, err := pfx.Key("key1").Delete().Do(ctx, c)
		assert.NoError(t, err)
		assert.True(t, ok)
	}()

	// Wait for DELETE event
	assertDone(t, func() {
		expected := WatchEvent{}
		expected.Type = DeleteEvent
		expected.Kv = &mvccpb.KeyValue{
			Key: []byte("my/prefix/key1"),
		}
		resp := <-ch
		assert.False(t, resp.Created)
		assert.NoError(t, resp.InitErr)
		assert.Equal(t, WatchResponse{Events: []WatchEvent{expected}}, clearResponse(resp))
	}, "DELETE timeout")

	// Wait for all goroutines
	wg.Wait()

	// Channel should be closed by the context
	cancel()
	_, ok := <-ch
	assert.False(t, ok)
}

func TestWatchBackoff(t *testing.T) {
	t.Parallel()

	b := newWatchBackoff()
	b.RandomizationFactor = 0

	// Get all delays without sleep
	var delays []time.Duration
	for i := 0; i < 14; i++ {
		delay := b.NextBackOff()
		if delay == backoff.Stop {
			assert.Fail(t, "unexpected stop")
			break
		}
		delays = append(delays, delay)
	}

	// Assert
	assert.Equal(t, []time.Duration{
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		3200 * time.Millisecond,
		6400 * time.Millisecond,
		12800 * time.Millisecond,
		25600 * time.Millisecond,
		51200 * time.Millisecond,
		time.Minute,
		time.Minute,
		time.Minute,
	}, delays)
}

func clearResponse(resp WatchResponse) WatchResponse {
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

func assertDone(t *testing.T, blockingOp func(), msgAndArgs ...any) {
	t.Helper()

	doneCh := make(chan struct{})
	go func() {
		blockingOp()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		// Ok
	case <-time.After(5 * time.Second):
		assert.Fail(t, "asertDone timeout", msgAndArgs...)
	}
}
