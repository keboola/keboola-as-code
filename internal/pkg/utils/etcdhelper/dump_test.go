package etcdhelper

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDumpAll(t *testing.T) {
	t.Parallel()
	client := ClientForTest(t)

	// Put keys
	_, err := client.Put(context.Background(), "key1", "value1")
	assert.NoError(t, err)
	_, err = client.Put(context.Background(), "key2", "value2")
	assert.NoError(t, err)
	_, err = client.Put(context.Background(), "key3/key4", `{"foo1": "bar1", "foo2": ["bar2", "bar3"]}`)
	assert.NoError(t, err)

	// Dump
	dump, err := DumpAll(context.Background(), client)
	assert.NoError(t, err)

	expected := `
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
	assert.Equal(t, strings.TrimLeft(expected, "\n"), dump)
}
