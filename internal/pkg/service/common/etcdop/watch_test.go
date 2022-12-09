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

func TestTypedPrefix_Watch(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := etcdhelper.ClientForTest(t)
	pfx := typedPrefixForTest()

	// Create watcher
	errHandler := func(err error) {
		assert.FailNow(t, `unexpected watch error`, err.Error())
	}
	ch := pfx.Watch(ctx, c, errHandler, etcd.WithRev(0))

	// PUT key
	go func() {
		assert.NoError(t, pfx.Key("key1").Put("foo").Do(ctx, c))
	}()

	// Wait for PUT event
	putDone := make(chan struct{})
	go func() {
		fooVal := fooType("foo")
		assert.Equal(t, EventT[fooType]{
			Value: &fooVal,
			Event: &etcd.Event{
				Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("my/prefix/key1"),
					Value: []byte(`"foo"`),
				},
			},
		}, clearEvent(<-ch))
		close(putDone)
	}()
	assert.Eventually(t, func() bool {
		select {
		case <-putDone:
			return true
		default:
			return false
		}
	}, 5*time.Second, 50*time.Millisecond, "PUT timeout")

	// DELETE key
	go func() {
		ok, err := pfx.Key("key1").Delete().Do(ctx, c)
		assert.NoError(t, err)
		assert.True(t, ok)
	}()

	// Wait for DELETE event
	deleteDone := make(chan struct{})
	go func() {
		assert.Equal(t, EventT[fooType]{
			Event: &etcd.Event{
				Type: mvccpb.DELETE,
				Kv: &mvccpb.KeyValue{
					Key: []byte("my/prefix/key1"),
				},
			},
		}, clearEvent(<-ch))
		close(deleteDone)
	}()
	assert.Eventually(t, func() bool {
		select {
		case <-deleteDone:
			return true
		default:
			return false
		}
	}, 5*time.Second, 50*time.Millisecond, "DELETE timeout")
}

func clearEvent(event EventT[fooType]) EventT[fooType] {
	event.Header = nil
	event.PrevKv = nil
	event.Kv.CreateRevision = 0
	event.Kv.ModRevision = 0
	event.Kv.Version = 0
	event.Kv.Lease = 0
	return event
}
