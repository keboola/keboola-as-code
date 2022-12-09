package etcdop

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestTypedPrefix_Watch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	etcd := etcdhelper.ClientForTest(t)

	pfx := typedPrefixForTest()
	ch := pfx.Watch(ctx, etcd, func(err error) {
		assert.FailNow(t, err.Error())
	})
	go func() {
		key1 := pfx.Key("key1")
		err := key1.Put("foo").Do(ctx, etcd)
		assert.NoError(t, err)
	}()
	msg := <-ch
	fooVal := fooType("foo")
	assert.Equal(t, EventT[fooType]{
		Type:  mvccpb.PUT,
		Key:   "my/prefix/key1",
		Value: &fooVal,
	}, msg)

	go func() {
		key1 := pfx.Key("key1")
		_, err := key1.Delete().Do(ctx, etcd)
		assert.NoError(t, err)
	}()
	msg = <-ch
	assert.Equal(t, EventT[fooType]{
		Type: mvccpb.DELETE,
		Key:  "my/prefix/key1",
	}, msg)
}
