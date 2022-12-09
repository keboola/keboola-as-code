package etcdop

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestTypedPrefix_Watch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	c := etcdhelper.ClientForTest(t)

	pfx := typedPrefixForTest()
	ch := pfx.Watch(ctx, c, func(err error) {
		assert.FailNow(t, err.Error())
	})
	go func() {
		key1 := pfx.Key("key1")
		err := key1.Put("foo").Do(ctx, c)
		assert.NoError(t, err)
	}()
	msg := <-ch
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
	}, clearEvent(msg))

	go func() {
		key1 := pfx.Key("key1")
		_, err := key1.Delete().Do(ctx, c)
		assert.NoError(t, err)
	}()
	msg = <-ch
	assert.Equal(t, EventT[fooType]{
		Event: &etcd.Event{
			Type: mvccpb.DELETE,
			Kv: &mvccpb.KeyValue{
				Key: []byte("my/prefix/key1"),
			},
		},
	}, clearEvent(msg))
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
