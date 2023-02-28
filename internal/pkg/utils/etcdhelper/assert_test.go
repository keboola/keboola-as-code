package etcdhelper_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

type mockedT struct {
	buf bytes.Buffer
}

// Errorf implements TestingT.
func (t *mockedT) Errorf(format string, args ...interface{}) {
	s := fmt.Sprintf(format, args...)
	t.buf.WriteString(s)
}

func TestAssertKVsString_Equal(t *testing.T) {
	t.Parallel()
	client := etcdhelper.ClientForTest(t)

	// Put keys
	ctx := context.Background()
	_, err := client.Put(ctx, "key1", "value1")
	assert.NoError(t, err)
	_, err = client.Put(ctx, "key2", "value2")
	assert.NoError(t, err)

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

func TestAssertKVsString_Difference(t *testing.T) {
	t.Parallel()
	client := etcdhelper.ClientForTest(t)

	// Put keys
	ctx := context.Background()
	_, err := client.Put(ctx, "key1", "value1")
	assert.NoError(t, err)
	_, err = client.Put(ctx, "key2", "value2")
	assert.NoError(t, err)

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
	            	"key1":
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
	            	"key2":
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
	client := etcdhelper.ClientForTest(t)

	// Put keys
	ctx := context.Background()
	_, err := client.Put(ctx, "key1", "value1")
	assert.NoError(t, err)
	_, err = client.Put(ctx, "key2", "value2")
	assert.NoError(t, err)

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
	client := etcdhelper.ClientForTest(t)

	// Put keys
	ctx := context.Background()
	_, err := client.Put(ctx, "key1", "value1")
	assert.NoError(t, err)

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
