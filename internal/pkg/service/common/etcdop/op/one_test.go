package op

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestGetOneOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	factory := func(ctx context.Context) (etcd.Op, error) {
		return etcd.OpGet("foo"), nil
	}
	mapper := func(ctx context.Context, raw *RawResponse) (*KeyValue, error) {
		get := raw.Get()
		if get.Count > 0 {
			return get.Kvs[0], nil
		}
		return nil, nil
	}

	v, err := NewGetOneOp(client, factory, mapper).Do(ctx).ResultOrErr()
	assert.NoError(t, err)
	assert.Nil(t, v)

	_, err = client.Put(ctx, "foo", "test1")
	assert.NoError(t, err)

	v, err = NewGetOneOp(client, factory, mapper).Do(ctx).ResultOrErr()
	assert.NoError(t, err)
	assert.NotNil(t, v)
	assert.Equal(t, "test1", string(v.Value))

	_, err = client.Put(ctx, "foo", "test2")
	assert.NoError(t, err)

	v, err = NewGetOneOp(client, factory, mapper).Do(ctx).ResultOrErr()
	assert.NoError(t, err)
	assert.NotNil(t, v)
	assert.Equal(t, "test2", string(v.Value))
}

func TestGetOneTOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	type Data struct {
		Field string `json:"field"`
	}

	factoryFn := func(ctx context.Context) (etcd.Op, error) {
		return etcd.OpGet("foo"), nil
	}

	mapper := func(ctx context.Context, raw *RawResponse) (*KeyValueT[Data], error) {
		get := raw.Get()
		if get.Count > 0 {
			kv := get.Kvs[0]
			value := Data{}
			err := json.DecodeString(string(kv.Value), &value)
			if err != nil {
				return nil, err
			}

			return &KeyValueT[Data]{
				Value: value,
				Kv:    kv,
			}, nil
		}
		return nil, nil
	}

	v, err := NewGetOneTOp(client, factoryFn, mapper).Do(ctx).ResultOrErr()
	assert.NoError(t, err)
	assert.Nil(t, v)

	_, err = client.Put(ctx, "foo", json.MustEncodeString(Data{Field: "test1"}, false))
	assert.NoError(t, err)

	v, err = NewGetOneTOp(client, factoryFn, mapper).Do(ctx).ResultOrErr()
	assert.NoError(t, err)
	assert.NotNil(t, v)
	assert.Equal(t, Data{Field: "test1"}, v.Value)

	_, err = client.Put(ctx, "foo", json.MustEncodeString(Data{Field: "test2"}, false))
	assert.NoError(t, err)

	v, err = NewGetOneTOp(client, factoryFn, mapper).Do(ctx).ResultOrErr()
	assert.NoError(t, err)
	assert.NotNil(t, v)
	assert.Equal(t, Data{Field: "test2"}, v.Value)
}
