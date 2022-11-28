package op

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestCountOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t)

	factory := func(ctx context.Context) (etcd.Op, error) {
		return etcd.OpGet("test", etcd.WithPrefix()), nil
	}

	mapper := func(ctx context.Context, r etcd.OpResponse) (int64, error) {
		return r.Get().Count, nil
	}

	count, err := NewCountOp(factory, mapper).Do(ctx, client)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)

	_, err = client.Put(ctx, "test/0", "test0")
	assert.NoError(t, err)

	count, err = NewCountOp(factory, mapper).Do(ctx, client)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	_, err = client.Put(ctx, "test/1", "test1")
	assert.NoError(t, err)

	count, err = NewCountOp(factory, mapper).Do(ctx, client)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
}
