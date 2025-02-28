package etcdhelper_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

type mockedT struct {
	buf bytes.Buffer
}

// Errorf implements TestingT.
func (t *mockedT) Errorf(format string, args ...any) {
	s := fmt.Sprintf(format, args...)
	t.buf.WriteString(s)
}

func TestAssertKVsString_Equal(t *testing.T) {
	t.Parallel()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Put keys
	ctx := t.Context()
	_, err := client.Put(ctx, "key1", "value1")
	require.NoError(t, err)
	_, err = client.Put(ctx, "key2", "value2")
	require.NoError(t, err)

	// No error is expected
	etcdhelper.AssertKVsString(t, client, `
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
`)
}

func TestAssertKVsString_Equal_WithIgnoredKeyPattern(t *testing.T) {
	t.Parallel()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Put keys
	ctx := t.Context()
	_, err := client.Put(ctx, "key1", "value1")
	require.NoError(t, err)
	_, err = client.Put(ctx, "foo123", "bar")
	require.NoError(t, err)

	// No error is expected
	etcdhelper.AssertKVsString(t, client, `
<<<<<
key1
-----
value1
>>>>>
`, etcdhelper.WithIgnoredKeyPattern(`^foo.+`))
}

func TestAssertKVsString_Difference(t *testing.T) {
	t.Parallel()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Put keys
	ctx := t.Context()
	_, err := client.Put(ctx, "key1", "value1")
	require.NoError(t, err)
	_, err = client.Put(ctx, "key2", "value2")
	require.NoError(t, err)

	mT := &mockedT{}
	etcdhelper.AssertKVsString(mT, client, `
<<<<<
key1
-----
valueA
>>>>>

<<<<<
key2
-----
valueB
>>>>>
`)

	// Expected error
	wildcards.Assert(t, strings.TrimSpace(`
%AValue of the actual key
	            	"key1"
	            	doesn't match the expected key
	            	"key1"
	            	Diff:
	            	-----
	            	@@ -1 +1 @@
	            	-valueA
	            	+value1
	            	-----
	            	Actual:
	            	-----
	            	value1
	            	-----
	            	Expected:
	            	-----
	            	valueA
	            	-----

%AValue of the actual key
	            	"key2"
	            	doesn't match the expected key
	            	"key2"
	            	Diff:
	            	-----
	            	@@ -1 +1 @@
	            	-valueB
	            	+value2
	            	-----
	            	Actual:
	            	-----
	            	value2
	            	-----
	            	Expected:
	            	-----
	            	valueB
	            	-----
`), strings.TrimSpace(mT.buf.String()))
}

func TestAssertKVsString_OnlyInActual(t *testing.T) {
	t.Parallel()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Put keys
	ctx := t.Context()
	_, err := client.Put(ctx, "key1", "value1")
	require.NoError(t, err)
	_, err = client.Put(ctx, "key2", "value2")
	require.NoError(t, err)

	mT := &mockedT{}
	etcdhelper.AssertKVsString(mT, client, `
<<<<<
key1
-----
value1
>>>>>
`)

	// Expected error
	wildcards.Assert(t, `
%A
	Error:      	These keys are in actual but not expected ectd state:
	            	[001] key2
`, strings.TrimSpace(mT.buf.String()))
}

func TestAssertKVsString_OnlyInExpected(t *testing.T) {
	t.Parallel()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Put keys
	ctx := t.Context()
	_, err := client.Put(ctx, "key1", "value1")
	require.NoError(t, err)

	mT := &mockedT{}
	etcdhelper.AssertKVsString(mT, client, `
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
`)

	// Expected error
	wildcards.Assert(t, `
%A
	Error:      	These keys are in expected but not actual ectd state:
	            	[001] key2
`, strings.TrimSpace(mT.buf.String()))
}

func TestAssertKVsFromFile_Difference(t *testing.T) {
	t.Parallel()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Put keys
	ctx := t.Context()
	_, err := client.Put(ctx, "key1", "value1")
	require.NoError(t, err)
	_, err = client.Put(ctx, "key2", "value2")
	require.NoError(t, err)

	mT := &mockedT{}
	etcdhelper.AssertKVsFromFile(mT, client, `fixtures/expected-001.txt`)

	// Expected error
	wildcards.Assert(t, strings.TrimSpace(`
%AValue of the actual key
	            	"key1"
	            	doesn't match the expected key
	            	"key1"
	            	defined in the file
	            	"fixtures/expected-001.txt"
	            	Diff:
	            	-----
	            	@@ -1 +1 @@
	            	-valueA
	            	+value1
	            	-----
	            	Actual:
	            	-----
	            	value1
	            	-----
	            	Expected:
	            	-----
	            	valueA
	            	-----

%AValue of the actual key
	            	"key2"
	            	doesn't match the expected key
	            	"key2"
	            	defined in the file
	            	"fixtures/expected-001.txt"
	            	Diff:
	            	-----
	            	@@ -1 +1 @@
	            	-valueB
	            	+value2
	            	-----
	            	Actual:
	            	-----
	            	value2
	            	-----
	            	Expected:
	            	-----
	            	valueB
	            	-----
`), strings.TrimSpace(mT.buf.String()))
}

func TestAssertKeys_Equal(t *testing.T) {
	t.Parallel()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Put keys
	ctx := t.Context()
	_, err := client.Put(ctx, "key1", "value1")
	require.NoError(t, err)
	_, err = client.Put(ctx, "key2", "value2")
	require.NoError(t, err)

	// No error is expected
	etcdhelper.AssertKeys(t, client, []string{"key1", "key2"})
}

func TestAssertKeys_Equal_WithIgnoredKeyPattern(t *testing.T) {
	t.Parallel()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Put keys
	ctx := t.Context()
	_, err := client.Put(ctx, "key1", "value1")
	require.NoError(t, err)
	_, err = client.Put(ctx, "foo123", "bar")
	require.NoError(t, err)

	// No error is expected
	etcdhelper.AssertKeys(t, client, []string{"key1"}, etcdhelper.WithIgnoredKeyPattern(`^foo.+`))
}

func TestAssertKeys_Difference(t *testing.T) {
	t.Parallel()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Put keys
	ctx := t.Context()
	_, err := client.Put(ctx, "key1", "value1")
	require.NoError(t, err)
	_, err = client.Put(ctx, "key2", "value2")
	require.NoError(t, err)

	mT := &mockedT{}
	etcdhelper.AssertKeys(mT, client, []string{"key1", "key3"})

	// Expected error
	wildcards.Assert(t, strings.TrimSpace(`
%A
	Error:      	These keys are in expected but not actual ectd state:
	            	[001] key3

%A
	Error:      	These keys are in actual but not expected ectd state:
	            	[001] key2
`), strings.TrimSpace(mT.buf.String()))
}

func TestAssertKeys_Wildcard(t *testing.T) {
	t.Parallel()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Put keys
	ctx := t.Context()
	_, err := client.Put(ctx, "key1", "value1")
	require.NoError(t, err)
	_, err = client.Put(ctx, "key2", "value2")
	require.NoError(t, err)

	// No error is expected
	etcdhelper.AssertKeys(t, client, []string{"key%d", "key%d"})
}
