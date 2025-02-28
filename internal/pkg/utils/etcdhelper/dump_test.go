package etcdhelper_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestDumpAll(t *testing.T) {
	t.Parallel()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Put keys
	_, err := client.Put(t.Context(), "key1", "value1")
	require.NoError(t, err)
	_, err = client.Put(t.Context(), "key2", "value2")
	require.NoError(t, err)
	_, err = client.Put(t.Context(), "key3/key4", `{"foo1": "bar1", "foo2": ["bar2", "bar3"]}`)
	require.NoError(t, err)

	// Dump
	dump, err := etcdhelper.DumpAllToString(t.Context(), client)
	require.NoError(t, err)

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

func TestDumpAllKeys(t *testing.T) {
	t.Parallel()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Put keys
	_, err := client.Put(t.Context(), "key1", "value1")
	require.NoError(t, err)
	_, err = client.Put(t.Context(), "key2", "value2")
	require.NoError(t, err)
	_, err = client.Put(t.Context(), "key3/key4", `{"foo1": "bar1", "foo2": ["bar2", "bar3"]}`)
	require.NoError(t, err)

	// Dump
	dump, err := etcdhelper.DumpAllKeys(t.Context(), client)
	require.NoError(t, err)
	assert.Equal(t, []string{"key1", "key2", "key3/key4"}, dump)
}
