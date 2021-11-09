package diff

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReporterStringsDiff(t *testing.T) {
	t.Parallel()
	cases := []struct{ remote, local, result string }{
		{"", "", ""},
		{"foo\n", "", "  - foo\n  - "},
		{"foo\n", "bar\n", "  - foo\n  + bar"},
		{"foo", "foo", ""},
		{"foo", "bar", "  - foo\n  + bar"},
		{"abc\nfoo", "abc\nbar", "    abc\n  - foo\n  + bar"},
		{"abc\nfoo\nabc", "abc\nbar\nabc", "    abc\n  - foo\n  + bar\n    abc"},
	}
	for i, c := range cases {
		result := stringsDiff(c.remote, c.local)
		assert.Equal(t, c.result, result, fmt.Sprintf(`case "%d"`, i))
	}
}

func TestReporterStringsDiffMaxEqualLines(t *testing.T) {
	t.Parallel()
	remote := `
xyz
foo1
foo2
foo3
foo4
foo5
foo6
foo7
foo8
foo9
abc
bar1
bar2
bar3
bar4
bar5
def
baz1
baz2
baz3
baz4
baz5
baz6
`

	local := `
foo1
foo2
foo3
foo4
foo5
foo6
foo7
foo8
foo9
123
bar1
bar2
bar3
bar4
bar5
baz1
baz2
baz3
baz4
baz5
baz6
`
	expected := `
  - xyz
    foo1
    foo2
    foo3
    foo4
    ...
  - abc
  + 123
    bar1
    bar2
    bar3
    bar4
    bar5
  - def
    baz1
    baz2
    baz3
    baz4
    ...
`

	assert.Equal(t, strings.Trim(expected, "\n"), stringsDiff(strings.Trim(remote, "\n"), strings.Trim(local, "\n")))
}
