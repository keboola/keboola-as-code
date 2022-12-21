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
		assert.Equal(t, expected, clearEvent(<-ch))
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
		assert.Equal(t, expected, clearEvent(<-ch))
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
		assert.Equal(t, expected, clearEvent(<-ch))
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
	ch, initDone := pfx.GetAllAndWatch(ctx, c, errHandler)

	// Wait for CREATE key1 event
	assertDone(t, func() {
		expected := Event{}
		expected.Type = CreateEvent
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key1"),
			Value: []byte("foo1"),
		}
		assert.Equal(t, expected, clearEvent(<-ch))
	}, "CREATE1 timeout")

	// Init (GetAll) phase should be finished
	assertDone(t, func() {
		<-initDone
	}, "initDone timeout")

	// CREATE key2
	wg.Add(1)
	go func() {
		wg.Done()
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
		assert.Equal(t, expected, clearEvent(<-ch))
	}, "CREATE2 timeout")

	// UPDATE key
	wg.Add(1)
	go func() {
		wg.Done()
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
		assert.Equal(t, expected, clearEvent(<-ch))
	}, "UPDATE timeout")

	// DELETE key
	wg.Add(1)
	go func() {
		wg.Done()
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
		assert.Equal(t, expected, clearEvent(<-ch))
	}, "DELETE timeout")

	wg.Wait()
}

func TestPrefixT_Watch(t *testing.T) {
	t.Parallel()

	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := etcdhelper.ClientForTest(t)
	pfx := typedPrefixForTest()

	// Create watcher
	errHandler := func(err error) {
		assert.FailNow(t, `unexpected watch error`, err.Error())
	}
	ch := pfx.Watch(ctx, c, errHandler, etcd.WithRev(1)) // rev=1, always include complete history

	// CREATE key
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, pfx.Key("key1").Put("foo").Do(ctx, c))
	}()

	// Wait for CREATE event
	assertDone(t, func() {
		expected := EventT[fooType]{}
		expected.Value = "foo"
		expected.Type = CreateEvent
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key1"),
			Value: []byte(`"foo"`),
		}
		assert.Equal(t, expected, clearEventT(<-ch))
	}, "CREATE timeout")

	// UPDATE key
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, pfx.Key("key1").Put("new").Do(ctx, c))
	}()

	// Wait for UPDATE event
	assertDone(t, func() {
		expected := EventT[fooType]{}
		expected.Value = "new"
		expected.Type = UpdateEvent
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key1"),
			Value: []byte(`"new"`),
		}
		assert.Equal(t, expected, clearEventT(<-ch))
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
		expected := EventT[fooType]{}
		expected.Type = DeleteEvent
		expected.Kv = &mvccpb.KeyValue{
			Key: []byte("my/prefix/key1"),
		}
		assert.Equal(t, expected, clearEventT(<-ch))
	}, "DELETE timeout")

	wg.Wait()
}

func TestPrefixT_GetAllAndWatch(t *testing.T) {
	t.Parallel()

	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := etcdhelper.ClientForTest(t)
	pfx := typedPrefixForTest()

	// CREATE key1
	assert.NoError(t, pfx.Key("key1").Put("foo1").Do(ctx, c))

	// Create watcher
	errHandler := func(err error) {
		assert.FailNow(t, `unexpected watch error`, err.Error())
	}
	ch, initDone := pfx.GetAllAndWatch(ctx, c, errHandler, etcd.WithPrevKV())

	// Wait for CREATE key1 event
	assertDone(t, func() {
		expected := EventT[fooType]{}
		expected.Value = "foo1"
		expected.Type = CreateEvent
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key1"),
			Value: []byte(`"foo1"`),
		}
		assert.Equal(t, expected, clearEventT(<-ch))
	}, "CREATE1 timeout")

	// Init (GetAll) phase should be finished
	assertDone(t, func() {
		<-initDone
	}, "initDone timeout")

	// CREATE key2
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, pfx.Key("key2").Put("foo2").Do(ctx, c))
	}()

	// Wait for CREATE key1 event
	assertDone(t, func() {
		expected := EventT[fooType]{}
		expected.Value = "foo2"
		expected.Type = CreateEvent
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key2"),
			Value: []byte(`"foo2"`),
		}
		assert.Equal(t, expected, clearEventT(<-ch))
	}, "CREATE2 timeout")

	// UPDATE key
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, pfx.Key("key2").Put("new").Do(ctx, c))
	}()

	// Wait for UPDATE event
	assertDone(t, func() {
		expected := EventT[fooType]{}
		expected.Value = "new"
		expected.Type = UpdateEvent
		expected.Kv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key2"),
			Value: []byte(`"new"`),
		}
		expected.PrevKv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key2"),
			Value: []byte(`"foo2"`),
		}
		assert.Equal(t, expected, clearEventT(<-ch))
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
		expected := EventT[fooType]{}
		expected.Value = "foo1"
		expected.Type = DeleteEvent
		expected.Kv = &mvccpb.KeyValue{
			Key: []byte("my/prefix/key1"),
		}
		expected.PrevKv = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key1"),
			Value: []byte(`"foo1"`),
		}
		assert.Equal(t, expected, clearEventT(<-ch))
	}, "DELETE timeout")

	wg.Wait()
}

func clearEvent(event Event) Event {
	event.Header = nil
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
	return event
}

func clearEventT(event EventT[fooType]) EventT[fooType] {
	event.Header = nil
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
	return event
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
