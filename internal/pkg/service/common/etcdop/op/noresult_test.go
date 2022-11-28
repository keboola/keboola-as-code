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
	client := etcdhelper.ClientForTest(t)

	factory := func(ctx context.Context) (etcd.Op, error) {
		return etcd.OpGet("test"), nil
	}

	mapper := func(ctx context.Context, r etcd.OpResponse) error {
		return nil
	}

	err := NewNoResultOp(factory, mapper).Do(ctx, client)
	assert.NoError(t, err)

	_, err = client.Put(ctx, "foo", "test1")
	assert.NoError(t, err)

	err = NewNoResultOp(factory, mapper).Do(ctx, client)
	assert.NoError(t, err)
}
