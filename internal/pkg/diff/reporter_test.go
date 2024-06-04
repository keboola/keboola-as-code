package diff

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReporterValuesDiffSameType1(t *testing.T) {
	t.Parallel()
	out := valuesDiff(reflect.ValueOf(`123`), reflect.ValueOf(`456`))
	assert.Equal(t, []string{
		`- 123`,
		`+ 456`,
	}, out)
}

func TestReporterValuesDiffSameType2(t *testing.T) {
	t.Parallel()
	out := valuesDiff(reflect.ValueOf([]int{1, 2}), reflect.ValueOf([]int{3, 4}))
	assert.Equal(t, []string{
		`- [1 2]`,
		`+ [3 4]`,
	}, out)
}

func TestReporterValuesDiffSameTypeInterface(t *testing.T) {
	t.Parallel()
	out := valuesDiff(reflect.ValueOf(any([]int{1, 2})), reflect.ValueOf(any([]int{3, 4})))
	assert.Equal(t, []string{
		`- [1 2]`,
		`+ [3 4]`,
	}, out)
}

func TestReporterValuesDiffDifferentType1(t *testing.T) {
	t.Parallel()
	out := valuesDiff(reflect.ValueOf(123), reflect.ValueOf(`456`))
	assert.Equal(t, []string{
		`- 123`,
		`+ "456"`,
	}, out)
}

func TestReporterValuesDiffDifferentType2(t *testing.T) {
	t.Parallel()
	out := valuesDiff(reflect.ValueOf([]float64{1, 2}), reflect.ValueOf([]int{1, 2}))
	assert.Equal(t, []string{
		`- []float64{1, 2}`,
		`+ []int{1, 2}`,
	}, out)
}

func TestReporterValuesDiffDifferentTypeInterface(t *testing.T) {
	t.Parallel()
	out := valuesDiff(reflect.ValueOf(any([]float64{1, 2})), reflect.ValueOf(any([]int{1, 2})))
	assert.Equal(t, []string{
		`- []float64{1, 2}`,
		`+ []int{1, 2}`,
	}, out)
}

func TestReporterStringsDiff(t *testing.T) {
	t.Parallel()
	cases := []struct{ remote, local, result string }{
		{"", "", ""},
		{"foo\n", "", "- foo\n- "},
		{"foo\n", "bar\n", "- foo\n+ bar"},
		{"foo", "foo", ""},
		{"foo", "bar", "- foo\n+ bar"},
		{"abc\nfoo", "abc\nbar", "  abc\n- foo\n+ bar"},
		{"abc\nfoo\nabc", "abc\nbar\nabc", "  abc\n- foo\n+ bar\n  abc"},
	}
	for i, c := range cases {
		result := stringsDiff(c.remote, c.local)
		assert.Equal(t, c.result, result, `case "%d"`, i)
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
