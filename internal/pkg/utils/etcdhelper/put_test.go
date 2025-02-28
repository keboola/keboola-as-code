package etcdhelper_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestPutAllFromSnapshot(t *testing.T) {
	t.Parallel()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	snapshot := `
<<<<<
key1
-----
value1
>>>>>

<<<<<
key2 (lease)
-----
value2
>>>>>

<<<<<
key3
-----
value3
>>>>>

<<<<<
key3/key4
-----
{
  "foo1": "bar1",
  "foo2": [
    "bar2",
    "bar3"
  ]
}
>>>>>
`

	// PUT
	err := etcdhelper.PutAllFromSnapshot(t.Context(), client, snapshot)
	require.NoError(t, err)

	// Put keys
	res, err := client.Get(t.Context(), "key1")
	require.NoError(t, err)
	assert.Len(t, res.Kvs, 1)
	assert.Equal(t, "value1", string(res.Kvs[0].Value))
	assert.Equal(t, int64(0), res.Kvs[0].Lease)

	res, err = client.Get(t.Context(), "key2")
	require.NoError(t, err)
	assert.Len(t, res.Kvs, 1)
	assert.Equal(t, "value2", string(res.Kvs[0].Value))
	assert.Positive(t, res.Kvs[0].Lease, int64(0))

	res, err = client.Get(t.Context(), "key3")
	require.NoError(t, err)
	assert.Len(t, res.Kvs, 1)
	assert.Equal(t, "value3", string(res.Kvs[0].Value))
	assert.Equal(t, int64(0), res.Kvs[0].Lease)

	res, err = client.Get(t.Context(), "key3/key4")
	require.NoError(t, err)
	assert.Len(t, res.Kvs, 1)
	assert.JSONEq(t, `{"foo1": "bar1","foo2": ["bar2","bar3"]}`, string(res.Kvs[0].Value))
}
