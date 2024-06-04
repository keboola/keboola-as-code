package op

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestCountOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	factoryFn := func(ctx context.Context) (etcd.Op, error) {
		return etcd.OpGet("test", etcd.WithPrefix()), nil
	}

	mapper := func(ctx context.Context, raw *RawResponse) (int64, error) {
		return raw.Get().Count, nil
	}

	count, err := NewCountOp(client, factoryFn, mapper).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	_, err = client.Put(ctx, "test/0", "test0")
	require.NoError(t, err)

	count, err = NewCountOp(client, factoryFn, mapper).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	_, err = client.Put(ctx, "test/1", "test1")
	require.NoError(t, err)

	count, err = NewCountOp(client, factoryFn, mapper).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)
}
