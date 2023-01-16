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

func TestPrefix_GetAllAndWatch_ErrCompacted(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Both clients must use same namespace
	etcdNamespace := "unit-" + t.Name() + "-" + gonanoid.Must(8)

	// Create client for the test
	testClient := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)

	// Create watcher client with custom dialer
	var conn net.Conn
	dialerLock := &sync.Mutex{}
	dialer := func(ctx context.Context, s string) (net.Conn, error) {
		dialerLock.Lock()
		defer dialerLock.Unlock()
		var err error
		conn, err = (&net.Dialer{}).DialContext(ctx, "tcp", s)
		return conn, err
	}
	watchClient := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace, grpc.WithContextDialer(dialer))

	// Create watcher
	pfx := prefixForTest()
	ch := pfx.GetAllAndWatch(ctx, watchClient)
	receive := func(expectedLen int) WatchResponse {
		resp := <-ch
		assert.False(t, resp.Created)
		assert.False(t, resp.Restarted)
		assert.NoError(t, resp.InitErr)
		assert.NoError(t, resp.Err)
		assert.Len(t, resp.Events, expectedLen)
		return resp
	}

	// Expect "created" event, there is no record for GetAll phase, transition to the Watch phase
	resp := <-ch
	assert.True(t, resp.Created)

	// Add some key
	value := "value"
	assert.NoError(t, pfx.Key("key01").Put(value).Do(ctx, testClient))

	// Read key
	assert.Equal(t, []byte("my/prefix/key01"), receive(1).Events[0].Kv.Key)

	// Close watcher connection and block a new one
	dialerLock.Lock()
	assert.NoError(t, conn.Close())

	// Add some other keys, during the watcher is disconnected
	assert.NoError(t, pfx.Key("key02").Put(value).Do(ctx, testClient))
	assert.NoError(t, pfx.Key("key03").Put(value).Do(ctx, testClient))

	// Compact, during the watcher is disconnected
	status, err := testClient.Status(ctx, testClient.Endpoints()[0])
	assert.NoError(t, err)
	_, err = testClient.Compact(ctx, status.Header.Revision)
	assert.NoError(t, err)

	// Unblock dialer, watcher will be reconnected
	dialerLock.Unlock()

	// Expect ErrCompacted, all the keys were merged into one revision, it is not possible to load only the missing ones
	resp = <-ch
	assert.Error(t, resp.Err)
	assert.Equal(t, "etcdserver: mvcc: required revision has been compacted", resp.Err.Error())

	// Expect "restarted" event
	resp = <-ch
	assert.True(t, resp.Restarted)
	wildcards.Assert(t, "restarted after %s, reason: etcdserver: mvcc: required revision has been compacted", resp.RestartReason)

	// Read keys, watcher was restarted, it is now in the GetAll phase,
	// so all keys are received at once
	resp = receive(3)
	assert.Equal(t, []byte("my/prefix/key01"), resp.Events[0].Kv.Key)
	assert.Equal(t, []byte("my/prefix/key02"), resp.Events[1].Kv.Key)
	assert.Equal(t, []byte("my/prefix/key03"), resp.Events[2].Kv.Key)

	// Add key
	assert.NoError(t, pfx.Key("key04").Put(value).Do(ctx, testClient))

	// Read keys
	assert.Equal(t, []byte("my/prefix/key04"), receive(1).Events[0].Kv.Key)

	// And let's try compact operation again, in the same way
	dialerLock.Lock()
	assert.NoError(t, conn.Close())
	assert.NoError(t, pfx.Key("key05").Put(value).Do(ctx, testClient))
	assert.NoError(t, pfx.Key("key06").Put(value).Do(ctx, testClient))
	status, err = testClient.Status(ctx, testClient.Endpoints()[0])
	assert.NoError(t, err)
	_, err = testClient.Compact(ctx, status.Header.Revision)
	assert.NoError(t, err)
	dialerLock.Unlock()
	resp = <-ch
	assert.Error(t, resp.Err)
	assert.Equal(t, "etcdserver: mvcc: required revision has been compacted", resp.Err.Error())
	resp = <-ch
	assert.True(t, resp.Restarted)
	wildcards.Assert(t, "restarted after %s, reason: etcdserver: mvcc: required revision has been compacted", resp.RestartReason)
	resp = receive(6)
	assert.Equal(t, []byte("my/prefix/key01"), resp.Events[0].Kv.Key)
	assert.Equal(t, []byte("my/prefix/key02"), resp.Events[1].Kv.Key)
	assert.Equal(t, []byte("my/prefix/key03"), resp.Events[2].Kv.Key)
	assert.Equal(t, []byte("my/prefix/key04"), resp.Events[3].Kv.Key)
	assert.Equal(t, []byte("my/prefix/key05"), resp.Events[4].Kv.Key)
	assert.Equal(t, []byte("my/prefix/key06"), resp.Events[5].Kv.Key)

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
