package etcdop

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/davecgh/go-spew/spew"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/tests/v3/integration"
	"google.golang.org/grpc/connectivity"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestPrefix_Watch(t *testing.T) {
	t.Parallel()

	wg := sync.WaitGroup{}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	pfx := prefixForTest()

	// Create watcher
	stream := pfx.Watch(ctx, client)
	ch := stream.Channel()

	// Wait for watcher created event
	assertDone(t, func() {
		resp := <-ch
		assert.True(t, resp.Created)
		require.NoError(t, resp.InitErr)
		assert.Empty(t, resp.Events)
	}, "watcher created timeout")

	// CREATE key
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, pfx.Key("key1").Put(client, "foo").Do(ctx).Err())
	}()

	// Wait for CREATE event
	assertDone(t, func() {
		expected := WatchEvent[[]byte]{}
		expected.Type = CreateEvent
		expected.Key = "my/prefix/key1"
		expected.Value = []byte("foo")
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key1"),
			Value: []byte("foo"),
		}
		resp := <-ch
		assert.False(t, resp.Created)
		require.NoError(t, resp.InitErr)
		assert.Equal(t, WatchResponseRaw{Events: []WatchEvent[[]byte]{expected}}, clearResponse(resp))
	}, "CREATE timeout")

	// UPDATE key
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, pfx.Key("key1").Put(client, "new").Do(ctx).Err())
	}()

	// Wait for UPDATE event
	assertDone(t, func() {
		expected := WatchEvent[[]byte]{}
		expected.Type = UpdateEvent
		expected.Key = "my/prefix/key1"
		expected.Value = []byte("new")
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key1"),
			Value: []byte("new"),
		}
		resp := <-ch
		assert.False(t, resp.Created)
		require.NoError(t, resp.InitErr)
		assert.Equal(t, WatchResponseRaw{Events: []WatchEvent[[]byte]{expected}}, clearResponse(resp))
	}, "UPDATE timeout")

	// DELETE key
	wg.Add(1)
	go func() {
		defer wg.Done()
		ok, err := pfx.Key("key1").Delete(client).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, ok)
	}()

	// Wait for DELETE event
	assertDone(t, func() {
		expected := WatchEvent[[]byte]{}
		expected.Type = DeleteEvent
		expected.Key = "my/prefix/key1"
		expected.Kv = &mvccpb.KeyValue{
			Key: []byte("my/prefix/key1"),
		}
		resp := <-ch
		assert.False(t, resp.Created)
		require.NoError(t, resp.InitErr)
		assert.Equal(t, WatchResponseRaw{Events: []WatchEvent[[]byte]{expected}}, clearResponse(resp))
	}, "DELETE timeout")

	// Manual RESTART
	assertDone(t, func() {
		// Trigger manual restart
		stream.Restart(errors.New("some cause"))

		// Receive the restarted event
		resp := <-ch
		assert.True(t, resp.Restarted)
		if assert.Error(t, resp.RestartCause) {
			assert.Equal(t, "some cause", resp.RestartCause.Error())
		}

		// Receive the created event
		resp = <-ch
		assert.True(t, resp.Created)

		// Add a new key
		require.NoError(t, pfx.Key("key3").Put(client, "new").Do(ctx).Err())

		// Receive the new key
		resp = <-ch
		if assert.Len(t, resp.Events, 1) {
			assert.Equal(t, []byte("my/prefix/key3"), resp.Events[0].Kv.Key)
		}
	}, "RESTART timeout")

	// Wait for all goroutines
	wg.Wait()

	// Channel should be closed by the context
	cancel()
	resp, ok := <-ch
	assert.False(t, ok, spew.Sdump(resp))
}

func TestPrefix_GetAllAndWatch(t *testing.T) {
	t.Parallel()

	wg := sync.WaitGroup{}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	pfx := prefixForTest()

	// CREATE key1
	require.NoError(t, pfx.Key("key1").Put(client, "foo1").Do(ctx).Err())

	// Create watcher
	stream := pfx.GetAllAndWatch(ctx, client)
	ch := stream.Channel()

	// Wait for CREATE key1 event
	assertDone(t, func() {
		expected := WatchEvent[[]byte]{}
		expected.Type = CreateEvent
		expected.Key = "my/prefix/key1"
		expected.Value = []byte("foo1")
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key1"),
			Value: []byte("foo1"),
		}
		resp := <-ch
		assert.False(t, resp.Created)
		require.NoError(t, resp.InitErr)
		assert.Equal(t, WatchResponseRaw{Events: []WatchEvent[[]byte]{expected}}, clearResponse(resp))
	}, "CREATE1 timeout")

	// Wait for watcher created event
	assertDone(t, func() {
		resp := <-ch
		assert.True(t, resp.Created)
		require.NoError(t, resp.InitErr)
		assert.Empty(t, resp.Events)
	}, "watcher created timeout")

	// CREATE key2
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, pfx.Key("key2").Put(client, "foo2").Do(ctx).Err())
	}()

	// Wait for CREATE key1 event
	assertDone(t, func() {
		expected := WatchEvent[[]byte]{}
		expected.Type = CreateEvent
		expected.Key = "my/prefix/key2"
		expected.Value = []byte("foo2")
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key2"),
			Value: []byte("foo2"),
		}
		resp := <-ch
		assert.False(t, resp.Created)
		require.NoError(t, resp.InitErr)
		assert.Equal(t, WatchResponseRaw{Events: []WatchEvent[[]byte]{expected}}, clearResponse(resp))
	}, "CREATE2 timeout")

	// UPDATE key
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, pfx.Key("key2").Put(client, "new").Do(ctx).Err())
	}()

	// Wait for UPDATE event
	assertDone(t, func() {
		expected := WatchEvent[[]byte]{}
		expected.Type = UpdateEvent
		expected.Key = "my/prefix/key2"
		expected.Value = []byte("new")
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key2"),
			Value: []byte("new"),
		}
		resp := <-ch
		assert.False(t, resp.Created)
		require.NoError(t, resp.InitErr)
		assert.Equal(t, WatchResponseRaw{Events: []WatchEvent[[]byte]{expected}}, clearResponse(resp))
	}, "UPDATE timeout")

	// DELETE key
	wg.Add(1)
	go func() {
		defer wg.Done()
		ok, err := pfx.Key("key1").Delete(client).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, ok)
	}()

	// Wait for DELETE event
	assertDone(t, func() {
		expected := WatchEvent[[]byte]{}
		expected.Type = DeleteEvent
		expected.Key = "my/prefix/key1"
		expected.Kv = &mvccpb.KeyValue{
			Key: []byte("my/prefix/key1"),
		}
		resp := <-ch
		assert.False(t, resp.Created)
		require.NoError(t, resp.InitErr)
		assert.Equal(t, WatchResponseRaw{Events: []WatchEvent[[]byte]{expected}}, clearResponse(resp))
	}, "DELETE timeout")

	// Manual RESTART
	assertDone(t, func() {
		// Trigger manual restart
		stream.Restart(errors.New("some cause"))

		// Receive the restart event
		resp := <-ch
		assert.True(t, resp.Restarted)
		if assert.Error(t, resp.RestartCause) {
			assert.Equal(t, "some cause", resp.RestartCause.Error())
		}

		// Receive all keys
		resp = <-ch
		if assert.Len(t, resp.Events, 1) {
			assert.Equal(t, []byte("my/prefix/key2"), resp.Events[0].Kv.Key)
		}

		// Add a new key
		require.NoError(t, pfx.Key("key3").Put(client, "new").Do(ctx).Err())

		// Receive the restarted event
		resp = <-ch
		assert.True(t, resp.Created)

		// Receive the new key
		resp = <-ch
		if assert.Len(t, resp.Events, 1) {
			assert.Equal(t, []byte("my/prefix/key3"), resp.Events[0].Kv.Key)
		}
	}, "RESTART timeout")

	// Wait for all goroutines
	wg.Wait()

	// Channel should be closed by the context
	cancel()
	resp, ok := <-ch
	assert.False(t, ok, spew.Sdump(resp))
}

// nolint:paralleltest // etcd integration tests cannot run in parallel, see integration.BeforeTestExternal
func TestPrefix_Watch_ErrCompacted(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf(`etcd compact tests are tested only on Linux`)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	// Create etcd cluster for test
	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 3, UseBridge: true})
	defer cluster.Terminate(t)
	cluster.WaitLeader(t)
	testClient := cluster.Client(1)
	watchMember := cluster.Members[2]
	watchClient := cluster.Client(2)

	// Create watcher
	pfx := prefixForTest()
	stream := pfx.Watch(ctx, watchClient)
	ch := stream.Channel()
	receive := func(expectedLen int) WatchResponseRaw {
		resp, ok := <-ch
		assert.True(t, ok)
		assert.False(t, resp.Created)
		assert.False(t, resp.Restarted)
		require.NoError(t, resp.InitErr)
		require.NoError(t, resp.Err)
		assert.Len(t, resp.Events, expectedLen)
		return resp
	}

	// Expect "created" event, there is no record for GetAll phase, transition to the Watch phase
	resp := <-ch
	assert.True(t, resp.Created)

	// Add some key
	value := "value"
	require.NoError(t, pfx.Key("key01").Put(testClient, value).Do(ctx).Err())

	// Read key
	assert.Equal(t, []byte("my/prefix/key01"), receive(1).Events[0].Kv.Key)

	// Close watcher connection and block a new one
	watchMember.Bridge().PauseConnections()
	watchMember.Bridge().DropConnections()
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, connectivity.Connecting, watchClient.ActiveConnection().GetState())
	}, 5*time.Second, 100*time.Millisecond)

	// Add some other keys, during the watcher is disconnected
	require.NoError(t, pfx.Key("key02").Put(testClient, value).Do(ctx).Err())
	require.NoError(t, pfx.Key("key03").Put(testClient, value).Do(ctx).Err())

	// Compact, during the watcher is disconnected
	status, err := testClient.Status(ctx, testClient.Endpoints()[0])
	require.NoError(t, err)
	_, err = testClient.Compact(ctx, status.Header.Revision)
	require.NoError(t, err)

	// Unblock dialer, watcher will be reconnected
	watchMember.Bridge().UnpauseConnections()

	// Expect ErrCompacted, all the keys were merged into one revision, it is not possible to load only the missing ones
	resp = <-ch
	require.Error(t, resp.Err)
	assert.Equal(t, "watch error: etcdserver: mvcc: required revision has been compacted", resp.Err.Error())

	// Expect "restarted" event
	resp = <-ch
	assert.True(t, resp.Restarted)
	if assert.Error(t, resp.RestartCause) {
		wildcards.Assert(t, "unexpected restart, backoff delay %s, cause:\n- watch error: etcdserver: mvcc: required revision has been compacted", resp.RestartCause.Error())
	}

	// Expect "created" event
	resp = <-ch
	assert.True(t, resp.Created)

	// After the restart, Watch is waiting for new events, put and expected the key
	require.NoError(t, pfx.Key("key04").Put(testClient, value).Do(ctx).Err())
	assert.Equal(t, []byte("my/prefix/key04"), receive(1).Events[0].Kv.Key)

	// And let's try compact operation again, in the same way
	watchMember.Bridge().PauseConnections()
	watchMember.Bridge().DropConnections()
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, connectivity.Connecting, watchClient.ActiveConnection().GetState())
	}, 5*time.Second, 100*time.Millisecond)
	require.NoError(t, pfx.Key("key05").Put(testClient, value).Do(ctx).Err())
	require.NoError(t, pfx.Key("key06").Put(testClient, value).Do(ctx).Err())
	status, err = testClient.Status(ctx, testClient.Endpoints()[0])
	require.NoError(t, err)
	_, err = testClient.Compact(ctx, status.Header.Revision)
	require.NoError(t, err)
	watchMember.Bridge().UnpauseConnections()
	resp = <-ch
	require.Error(t, resp.Err)
	assert.Equal(t, "watch error: etcdserver: mvcc: required revision has been compacted", resp.Err.Error())
	resp = <-ch
	assert.True(t, resp.Restarted)
	if assert.Error(t, resp.RestartCause) {
		wildcards.Assert(t, "unexpected restart, backoff delay %s, cause:\n- watch error: etcdserver: mvcc: required revision has been compacted", resp.RestartCause.Error())
	}
	resp = <-ch
	assert.True(t, resp.Created)

	// After the restart, Watch is streaming new events, put and receive the key
	require.NoError(t, pfx.Key("key07").Put(testClient, value).Do(ctx).Err())
	assert.Equal(t, []byte("my/prefix/key07"), receive(1).Events[0].Kv.Key)

	// Channel should be closed by the context
	cancel()
	resp, ok := <-ch
	assert.False(t, ok, spew.Sdump(resp))
}

// nolint:paralleltest // etcd integration tests cannot run in parallel, see integration.BeforeTestExternal
func TestPrefix_GetAllAndWatch_ErrCompacted(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf(`etcd compact tests are tested only on Linux`)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	// Create etcd cluster for test
	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 3, UseBridge: true})
	defer cluster.Terminate(t)
	cluster.WaitLeader(t)
	testClient := cluster.Client(1)
	watchMember := cluster.Members[2]
	watchClient := cluster.Client(2)

	// Create watcher
	pfx := prefixForTest()
	stream := pfx.GetAllAndWatch(ctx, watchClient)
	ch := stream.Channel()
	receive := func(expectedLen int) WatchResponseRaw {
		resp, ok := <-ch
		assert.True(t, ok)
		assert.False(t, resp.Created)
		assert.False(t, resp.Restarted)
		require.NoError(t, resp.InitErr)
		require.NoError(t, resp.Err)
		assert.Len(t, resp.Events, expectedLen)
		return resp
	}

	// Expect "created" event, there is no record for GetAll phase, transition to the Watch phase
	resp := <-ch
	assert.True(t, resp.Created)

	// Add some key
	value := "value"
	require.NoError(t, pfx.Key("key01").Put(testClient, value).Do(ctx).Err())

	// Read key
	assert.Equal(t, []byte("my/prefix/key01"), receive(1).Events[0].Kv.Key)

	// Close watcher connection and block a new one
	watchMember.Bridge().PauseConnections()
	watchMember.Bridge().DropConnections()
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, connectivity.Connecting, watchClient.ActiveConnection().GetState())
	}, 5*time.Second, 100*time.Millisecond)

	// Add some other keys, during the watcher is disconnected
	require.NoError(t, pfx.Key("key02").Put(testClient, value).Do(ctx).Err())
	require.NoError(t, pfx.Key("key03").Put(testClient, value).Do(ctx).Err())

	// Compact, during the watcher is disconnected
	status, err := testClient.Status(ctx, testClient.Endpoints()[0])
	require.NoError(t, err)
	_, err = testClient.Compact(ctx, status.Header.Revision)
	require.NoError(t, err)

	// Unblock dialer, watcher will be reconnected
	watchMember.Bridge().UnpauseConnections()

	// Expect ErrCompacted, all the keys were merged into one revision, it is not possible to load only the missing ones
	resp = <-ch
	require.Error(t, resp.Err)
	assert.Equal(t, "watch error: etcdserver: mvcc: required revision has been compacted", resp.Err.Error())

	// Expect "restarted" event
	resp = <-ch
	assert.True(t, resp.Restarted)
	assert.True(t, resp.Restarted)
	if assert.Error(t, resp.RestartCause) {
		wildcards.Assert(t, "unexpected restart, backoff delay %s, cause:\n- watch error: etcdserver: mvcc: required revision has been compacted", resp.RestartCause.Error())
	}

	// Read keys, watcher was restarted, it is now in the GetAll phase,
	// so all keys are received at once
	resp = receive(3)
	assert.Equal(t, []byte("my/prefix/key01"), resp.Events[0].Kv.Key)
	assert.Equal(t, []byte("my/prefix/key02"), resp.Events[1].Kv.Key)
	assert.Equal(t, []byte("my/prefix/key03"), resp.Events[2].Kv.Key)

	// Add key
	require.NoError(t, pfx.Key("key04").Put(testClient, value).Do(ctx).Err())

	// Expect "created" event, transition from the GetAll to the Watch phase
	resp = <-ch
	assert.True(t, resp.Created)

	// Read keys
	assert.Equal(t, []byte("my/prefix/key04"), receive(1).Events[0].Kv.Key)

	// And let's try compact operation again, in the same way
	watchMember.Bridge().PauseConnections()
	watchMember.Bridge().DropConnections()
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, connectivity.Connecting, watchClient.ActiveConnection().GetState())
	}, 5*time.Second, 100*time.Millisecond)
	require.NoError(t, pfx.Key("key05").Put(testClient, value).Do(ctx).Err())
	require.NoError(t, pfx.Key("key06").Put(testClient, value).Do(ctx).Err())
	status, err = testClient.Status(ctx, testClient.Endpoints()[0])
	require.NoError(t, err)
	_, err = testClient.Compact(ctx, status.Header.Revision)
	require.NoError(t, err)
	watchMember.Bridge().UnpauseConnections()
	resp = <-ch
	require.Error(t, resp.Err)
	assert.Equal(t, "watch error: etcdserver: mvcc: required revision has been compacted", resp.Err.Error())
	resp = <-ch
	assert.True(t, resp.Restarted)
	if assert.Error(t, resp.RestartCause) {
		wildcards.Assert(t, "unexpected restart, backoff delay %s, cause:\n- watch error: etcdserver: mvcc: required revision has been compacted", resp.RestartCause.Error())
	}
	resp = receive(6)
	assert.Equal(t, []byte("my/prefix/key01"), resp.Events[0].Kv.Key)
	assert.Equal(t, []byte("my/prefix/key02"), resp.Events[1].Kv.Key)
	assert.Equal(t, []byte("my/prefix/key03"), resp.Events[2].Kv.Key)
	assert.Equal(t, []byte("my/prefix/key04"), resp.Events[3].Kv.Key)
	assert.Equal(t, []byte("my/prefix/key05"), resp.Events[4].Kv.Key)
	assert.Equal(t, []byte("my/prefix/key06"), resp.Events[5].Kv.Key)
	resp = <-ch
	assert.True(t, resp.Created)

	// Channel should be closed by the context
	cancel()
	resp, ok := <-ch
	assert.False(t, ok, spew.Sdump(resp))
}

func TestWatchBackoff(t *testing.T) {
	t.Parallel()

	b := newWatchBackoff()
	b.RandomizationFactor = 0

	// Get all delays without sleep
	delays := make([]time.Duration, 0, 14)
	for range 14 {
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

func clearResponse(resp WatchResponseRaw) WatchResponseRaw {
	for i := range resp.Events {
		event := &resp.Events[i]
		event.Key = string(event.Kv.Key)
		event.Value = event.Kv.Value
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

// nolint:paralleltest // etcd integration tests cannot run in parallel, see integration.BeforeTestExternal
func TestPrefix_Watch_ClusterDowntime(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf(`etcd cluster tests are tested only on Linux`)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	// Create etcd cluster for test
	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 3, UseBridge: true})
	defer cluster.Terminate(t)
	cluster.WaitLeader(t)
	testClient := cluster.Client(1)
	watchMember := cluster.Members[2]
	watchClient := cluster.Client(2)

	// Create watcher
	pfx := prefixForTest()
	stream := pfx.Watch(ctx, watchClient)
	ch := stream.Channel()
	receive := func(expectedLen int) WatchResponseRaw {
		resp, ok := <-ch
		assert.True(t, ok)
		assert.False(t, resp.Created)
		assert.False(t, resp.Restarted)
		require.NoError(t, resp.InitErr)
		require.NoError(t, resp.Err)
		assert.Len(t, resp.Events, expectedLen)
		return resp
	}

	// Expect "created" event, there is no record for GetAll phase, transition to the Watch phase
	resp := <-ch
	assert.True(t, resp.Created)

	// Add initial key
	value := "value"
	require.NoError(t, pfx.Key("key01").Put(testClient, value).Do(ctx).Err())
	require.NoError(t, pfx.Key("key02").Put(testClient, value).Do(ctx).Err())
	require.NoError(t, pfx.Key("key03").Put(testClient, value).Do(ctx).Err())

	// Read initial key
	resp = receive(1)
	assert.Equal(t, []byte("my/prefix/key01"), resp.Events[0].Kv.Key)
	resp = receive(1)
	assert.Equal(t, []byte("my/prefix/key02"), resp.Events[0].Kv.Key)
	resp = receive(1)
	assert.Equal(t, []byte("my/prefix/key03"), resp.Events[0].Kv.Key)

	// Close watcher connection and block a new one for 60 seconds
	watchMember.Bridge().PauseConnections()
	watchMember.Bridge().DropConnections()
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, connectivity.Connecting, watchClient.ActiveConnection().GetState())
	}, 5*time.Second, 100*time.Millisecond)

	// Add delete and create some keys during the downtime
	require.NoError(t, pfx.Key("key02").Delete(testClient).Do(ctx).Err())
	require.NoError(t, pfx.Key("key03").Delete(testClient).Do(ctx).Err())
	require.NoError(t, pfx.Key("key02").Put(testClient, value).Do(ctx).Err())
	require.NoError(t, pfx.Key("key03").Put(testClient, value).Do(ctx).Err())

	// Wait for 30 seconds to simulate extended downtime

	// Unblock dialer, watcher will be reconnected
	watchMember.Bridge().UnpauseConnections()

	// Should receive the old keys
	recv := receive(4)
	assert.Equal(t, []byte("my/prefix/key02"), recv.Events[0].Kv.Key)
	assert.Equal(t, DeleteEvent, recv.Events[0].Type)
	assert.Equal(t, []byte("my/prefix/key03"), recv.Events[1].Kv.Key)
	assert.Equal(t, DeleteEvent, recv.Events[1].Type)
	assert.Equal(t, []byte("my/prefix/key02"), recv.Events[2].Kv.Key)
	assert.Equal(t, CreateEvent, recv.Events[2].Type)
	assert.Equal(t, []byte("my/prefix/key03"), recv.Events[3].Kv.Key)
	assert.Equal(t, CreateEvent, recv.Events[3].Type)

	// Add another key after reconnection
	require.NoError(t, pfx.Key("key05").Put(testClient, value).Do(ctx).Err())

	// Should receive the new key
	assert.Equal(t, []byte("my/prefix/key05"), receive(1).Events[0].Kv.Key)

	// Channel should be closed by the context
	cancel()
	resp, ok := <-ch
	assert.False(t, ok, spew.Sdump(resp))
}
