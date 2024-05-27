package op

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestBoolOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	factoryFn := func(ctx context.Context) (etcd.Op, error) {
		return etcd.OpGet("test", etcd.WithPrefix()), nil
	}

	mapper := func(ctx context.Context, raw *RawResponse) (bool, error) {
		return raw.Get().Count > 0, nil
	}

	v, err := NewBoolOp(client, factoryFn, mapper).Do(ctx).ResultOrErr()
	assert.NoError(t, err)
	assert.False(t, v)

	_, err = client.Put(ctx, "test/0", "test0")
	assert.NoError(t, err)

	v, err = NewBoolOp(client, factoryFn, mapper).Do(ctx).ResultOrErr()
	assert.NoError(t, err)
	assert.True(t, v)

	_, err = client.Put(ctx, "test/1", "test1")
	assert.NoError(t, err)

	v, err = NewBoolOp(client, factoryFn, mapper).Do(ctx).ResultOrErr()
	assert.NoError(t, err)
	assert.True(t, v)
}
