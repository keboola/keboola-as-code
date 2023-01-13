package etcdop

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	etcd "go.etcd.io/etcd/client/v3"

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
	errHandler := func(err error) {
		assert.FailNow(t, `unexpected watch error`, err.Error())
	}
	ch := pfx.Watch(ctx, c, errHandler, etcd.WithRev(1)) // rev=1, always include complete history

	// Wait for watcher created event
	assertDone(t, func() {
		events := <-ch
		assert.True(t, events.Created)
		assert.NoError(t, events.InitErr)
		assert.Empty(t, events.Events)
	}, "watcher created timeout")

	// CREATE key
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, pfx.Key("key1").Put("foo").Do(ctx, c))
	}()

	// Wait for CREATE event
	assertDone(t, func() {
		expected := Event{}
		expected.Type = CreateEvent
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key1"),
			Value: []byte("foo"),
		}
		events := <-ch
		assert.False(t, events.Created)
		assert.NoError(t, events.InitErr)
		assert.Equal(t, Events{Events: []Event{expected}}, clearEvents(events))
	}, "CREATE timeout")

	// UPDATE key
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, pfx.Key("key1").Put("new").Do(ctx, c))
	}()

	// Wait for UPDATE event
	assertDone(t, func() {
		expected := Event{}
		expected.Type = UpdateEvent
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key1"),
			Value: []byte("new"),
		}
		events := <-ch
		assert.False(t, events.Created)
		assert.NoError(t, events.InitErr)
		assert.Equal(t, Events{Events: []Event{expected}}, clearEvents(events))
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
		expected := Event{}
		expected.Type = DeleteEvent
		expected.Kv = &mvccpb.KeyValue{
			Key: []byte("my/prefix/key1"),
		}
		events := <-ch
		assert.False(t, events.Created)
		assert.NoError(t, events.InitErr)
		assert.Equal(t, Events{Events: []Event{expected}}, clearEvents(events))
	}, "DELETE timeout")

	wg.Wait()
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
	errHandler := func(err error) {
		assert.FailNow(t, `unexpected watch error`, err.Error())
	}
	ch := pfx.GetAllAndWatch(ctx, c, errHandler)

	// Wait for CREATE key1 event
	assertDone(t, func() {
		expected := Event{}
		expected.Type = CreateEvent
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key1"),
			Value: []byte("foo1"),
		}
		events := <-ch
		assert.False(t, events.Created)
		assert.NoError(t, events.InitErr)
		assert.Equal(t, Events{Events: []Event{expected}}, clearEvents(events))
	}, "CREATE1 timeout")

	// Wait for watcher created event
	assertDone(t, func() {
		events := <-ch
		assert.True(t, events.Created)
		assert.NoError(t, events.InitErr)
		assert.Empty(t, events.Events)
	}, "watcher created timeout")

	// CREATE key2
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, pfx.Key("key2").Put("foo2").Do(ctx, c))
	}()

	// Wait for CREATE key1 event
	assertDone(t, func() {
		expected := Event{}
		expected.Type = CreateEvent
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key2"),
			Value: []byte("foo2"),
		}
		events := <-ch
		assert.False(t, events.Created)
		assert.NoError(t, events.InitErr)
		assert.Equal(t, Events{Events: []Event{expected}}, clearEvents(events))
	}, "CREATE2 timeout")

	// UPDATE key
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, pfx.Key("key2").Put("new").Do(ctx, c))
	}()

	// Wait for UPDATE event
	assertDone(t, func() {
		expected := Event{}
		expected.Type = UpdateEvent
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key2"),
			Value: []byte("new"),
		}
		events := <-ch
		assert.False(t, events.Created)
		assert.NoError(t, events.InitErr)
		assert.Equal(t, Events{Events: []Event{expected}}, clearEvents(events))
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
		expected := Event{}
		expected.Type = DeleteEvent
		expected.Kv = &mvccpb.KeyValue{
			Key: []byte("my/prefix/key1"),
		}
		events := <-ch
		assert.False(t, events.Created)
		assert.NoError(t, events.InitErr)
		assert.Equal(t, Events{Events: []Event{expected}}, clearEvents(events))
	}, "DELETE timeout")

	wg.Wait()
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

func clearEvents(events Events) Events {
	for i := range events.Events {
		event := &events.Events[i]
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
	events.Header = nil
	return events
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
