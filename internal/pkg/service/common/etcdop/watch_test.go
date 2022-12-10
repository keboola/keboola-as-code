package etcdop

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestPrefix_Watch(t *testing.T) {
	t.Parallel()

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
	go func() {
		assert.NoError(t, pfx.Key("key1").Put("foo").Do(ctx, c))
	}()

	// Wait for CREATE event
	assertDone(t, func() {
		expected := Event{}
		expected.Type = CreateEvent
		expected.KeyValue = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key1"),
			Value: []byte("foo"),
		}
		assert.Equal(t, expected, clearEvent(<-ch))
	}, "CREATE timeout")

	// UPDATE key
	go func() {
		assert.NoError(t, pfx.Key("key1").Put("new").Do(ctx, c))
	}()

	// Wait for UPDATE event
	assertDone(t, func() {
		expected := Event{}
		expected.Type = UpdateEvent
		expected.KeyValue = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key1"),
			Value: []byte("new"),
		}
		assert.Equal(t, expected, clearEvent(<-ch))
	}, "UPDATE timeout")

	// DELETE key
	go func() {
		ok, err := pfx.Key("key1").Delete().Do(ctx, c)
		assert.NoError(t, err)
		assert.True(t, ok)
	}()

	// Wait for DELETE event
	assertDone(t, func() {
		expected := Event{}
		expected.Type = DeleteEvent
		expected.KeyValue = &mvccpb.KeyValue{
			Key: []byte("my/prefix/key1"),
		}
		assert.Equal(t, expected, clearEvent(<-ch))
	}, "DELETE timeout")
}

func TestPrefixT_Watch(t *testing.T) {
	t.Parallel()

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
	go func() {
		assert.NoError(t, pfx.Key("key1").Put("foo").Do(ctx, c))
	}()

	// Wait for CREATE event
	assertDone(t, func() {
		expected := EventT[fooType]{}
		expected.Value = "foo"
		expected.Type = CreateEvent
		expected.KV = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key1"),
			Value: []byte(`"foo"`),
		}
		assert.Equal(t, expected, clearEventT(<-ch))
	}, "CREATE timeout")

	// UPDATE key
	go func() {
		assert.NoError(t, pfx.Key("key1").Put("new").Do(ctx, c))
	}()

	// Wait for UPDATE event
	assertDone(t, func() {
		expected := EventT[fooType]{}
		expected.Value = "new"
		expected.Type = UpdateEvent
		expected.KV = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key1"),
			Value: []byte(`"new"`),
		}
		assert.Equal(t, expected, clearEventT(<-ch))
	}, "UPDATE timeout")

	// DELETE key
	go func() {
		ok, err := pfx.Key("key1").Delete().Do(ctx, c)
		assert.NoError(t, err)
		assert.True(t, ok)
	}()

	// Wait for DELETE event
	assertDone(t, func() {
		expected := EventT[fooType]{}
		expected.Type = DeleteEvent
		expected.KV = &mvccpb.KeyValue{
			Key: []byte("my/prefix/key1"),
		}
		assert.Equal(t, expected, clearEventT(<-ch))
	}, "DELETE timeout")
}

func TestPrefixT_GetAllAndWatch(t *testing.T) {
	t.Parallel()

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
	ch := pfx.GetAllAndWatch(ctx, c, errHandler)

	// Wait for CREATE key1 event
	assertDone(t, func() {
		expected := EventT[fooType]{}
		expected.Value = "foo1"
		expected.Type = CreateEvent
		expected.KV = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key1"),
			Value: []byte(`"foo1"`),
		}
		assert.Equal(t, expected, clearEventT(<-ch))
	}, "CREATE1 timeout")

	// CREATE key2
	go func() {
		assert.NoError(t, pfx.Key("key2").Put("foo2").Do(ctx, c))
	}()

	// Wait for CREATE key1 event
	assertDone(t, func() {
		expected := EventT[fooType]{}
		expected.Value = "foo2"
		expected.Type = CreateEvent
		expected.KV = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key2"),
			Value: []byte(`"foo2"`),
		}
		assert.Equal(t, expected, clearEventT(<-ch))
	}, "CREATE2 timeout")

	// UPDATE key
	go func() {
		assert.NoError(t, pfx.Key("key2").Put("new").Do(ctx, c))
	}()

	// Wait for UPDATE event
	assertDone(t, func() {
		expected := EventT[fooType]{}
		expected.Value = "new"
		expected.Type = UpdateEvent
		expected.KV = &mvccpb.KeyValue{
			Key:   []byte("my/prefix/key2"),
			Value: []byte(`"new"`),
		}
		assert.Equal(t, expected, clearEventT(<-ch))
	}, "UPDATE timeout")

	// DELETE key
	go func() {
		ok, err := pfx.Key("key1").Delete().Do(ctx, c)
		assert.NoError(t, err)
		assert.True(t, ok)
	}()

	// Wait for DELETE event
	assertDone(t, func() {
		expected := EventT[fooType]{}
		expected.Type = DeleteEvent
		expected.KV = &mvccpb.KeyValue{
			Key: []byte("my/prefix/key1"),
		}
		assert.Equal(t, expected, clearEventT(<-ch))
	}, "DELETE timeout")
}

func clearEvent(event Event) Event {
	event.Header = nil
	event.KeyValue.CreateRevision = 0
	event.KeyValue.ModRevision = 0
	event.KeyValue.Version = 0
	event.KeyValue.Lease = 0
	return event
}

func clearEventT(event EventT[fooType]) EventT[fooType] {
	event.Header = nil
	event.KV.CreateRevision = 0
	event.KV.ModRevision = 0
	event.KV.Version = 0
	event.KV.Lease = 0
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
