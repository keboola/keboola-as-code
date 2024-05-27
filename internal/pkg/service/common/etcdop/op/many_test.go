package op

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestGetManyOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	factoryFn := func(ctx context.Context) (etcd.Op, error) {
		return etcd.OpGet("test", etcd.WithPrefix()), nil
	}

	mapper := func(ctx context.Context, raw *RawResponse) ([]*KeyValue, error) {
		return raw.Get().Kvs, nil
	}

	values, err := NewGetManyOp(client, factoryFn, mapper).Do(ctx).ResultOrErr()
	assert.NoError(t, err)
	assert.Empty(t, values)

	_, err = client.Put(ctx, "test/0", "test0")
	assert.NoError(t, err)

	values, err = NewGetManyOp(client, factoryFn, mapper).Do(ctx).ResultOrErr()
	assert.NoError(t, err)
	assert.Equal(t, []string{"test0"}, getStringValues(values))

	_, err = client.Put(ctx, "test/1", "test1")
	assert.NoError(t, err)

	values, err = NewGetManyOp(client, factoryFn, mapper).Do(ctx).ResultOrErr()
	assert.NoError(t, err)
	assert.Equal(t, []string{"test0", "test1"}, getStringValues(values))
}

func TestGetManyTOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	type Data struct {
		Field string `json:"field"`
	}

	factory := func(ctx context.Context) (etcd.Op, error) {
		return etcd.OpGet("test", etcd.WithPrefix()), nil
	}

	mapper := func(ctx context.Context, raw *RawResponse) (KeyValuesT[Data], error) {
		kvs := raw.Get().Kvs
		data := make(KeyValuesT[Data], 0, len(kvs))
		for _, kv := range kvs {
			value := Data{}
			err := json.DecodeString(string(kv.Value), &value)
			if err != nil {
				return nil, err
			}

			data = append(data, &KeyValueT[Data]{
				Value: value,
				Kv:    kv,
			})
		}
		return data, nil
	}

	values, err := NewGetManyTOp(client, factory, mapper).Do(ctx).ResultOrErr()
	assert.NoError(t, err)
	assert.Empty(t, values)

	_, err = client.Put(ctx, "test/0", json.MustEncodeString(Data{Field: "test0"}, false))
	assert.NoError(t, err)

	values, err = NewGetManyTOp(client, factory, mapper).Do(ctx).ResultOrErr()
	assert.NoError(t, err)
	assert.Equal(t, []Data{{Field: "test0"}}, values.Values())

	_, err = client.Put(ctx, "test/1", json.MustEncodeString(Data{Field: "test1"}, false))
	assert.NoError(t, err)

	values, err = NewGetManyTOp(client, factory, mapper).Do(ctx).ResultOrErr()
	assert.NoError(t, err)
	assert.Equal(t, []Data{{Field: "test0"}, {Field: "test1"}}, values.Values())
}

func getStringValues(kvs []*KeyValue) []string {
	r := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		r = append(r, string(kv.Value))
	}
	return r
}
