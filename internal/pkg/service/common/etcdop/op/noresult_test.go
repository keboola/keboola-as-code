package op

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestNoResultOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	factoryFn := func(ctx context.Context) (etcd.Op, error) {
		return etcd.OpGet("test"), nil
	}

	mapper := func(ctx context.Context, raw *RawResponse) error {
		return nil
	}

	err := NewNoResultOp(client, factoryFn, mapper).Do(ctx).Err()
	assert.NoError(t, err)

	_, err = client.Put(ctx, "foo", "test1")
	assert.NoError(t, err)

	assert.NoError(t, NewNoResultOp(client, factoryFn, mapper).Do(ctx).Err())
}
