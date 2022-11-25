package etcdhelper

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPutAllFromSnapshot(t *testing.T) {
	t.Parallel()
	client := ClientForTest(t)

	snapshot := `
<<<<<
key1
-----
value1
>>>>>

<<<<<
key2
-----
value2
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
	err := PutAllFromSnapshot(context.Background(), client, snapshot)
	assert.NoError(t, err)

	// Put keys
	res, err := client.Get(context.Background(), "key1")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(res.Kvs))
	assert.Equal(t, "value1", string(res.Kvs[0].Value))

	res, err = client.Get(context.Background(), "key2")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(res.Kvs))
	assert.Equal(t, "value2", string(res.Kvs[0].Value))

	res, err = client.Get(context.Background(), "key3/key4")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(res.Kvs))
	assert.Equal(t, "{\n  \"foo1\": \"bar1\",\n  \"foo2\": [\n    \"bar2\",\n    \"bar3\"\n  ]\n}", string(res.Kvs[0].Value))
}
