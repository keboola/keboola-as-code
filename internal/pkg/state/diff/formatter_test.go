package diff

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatter_FormatValue(t *testing.T) {
	t.Parallel()

	cases := []struct {
		a, b  reflect.Value
		lines []string
	}{
		// Same type 1
		{
			reflect.ValueOf(`123`),
			reflect.ValueOf(`456`),
			[]string{
				`- 123`,
				`+ 456`,
			},
		},
		// Same type 2
		{
			reflect.ValueOf([]int{1, 2}),
			reflect.ValueOf([]int{3, 4}),
			[]string{
				`- [1 2]`,
				`+ [3 4]`,
			},
		},
		// Same type wrapped by interface
		{
			reflect.ValueOf(interface{}([]int{1, 2})),
			reflect.ValueOf(interface{}([]int{3, 4})),
			[]string{
				`- [1 2]`,
				`+ [3 4]`,
			},
		},
		// Different types 1
		{
			reflect.ValueOf(123),
			reflect.ValueOf("456"),
			[]string{
				`- 123`,
				`+ "456"`,
			},
		},
		// Different types 2
		{
			reflect.ValueOf([]float64{1, 2}),
			reflect.ValueOf([]int{1, 2}),
			[]string{
				`- []float64{1, 2}`,
				`+ []int{1, 2}`,
			},
		},
		// Different types wrapped by interface
		{
			reflect.ValueOf(interface{}([]float64{1, 2})),
			reflect.ValueOf(interface{}([]int{1, 2})),
			[]string{
				`- []float64{1, 2}`,
				`+ []int{1, 2}`,
			},
		},
	}

	for i, c := range cases {
		f := newFormatter()
		f.formatValue(&ResultItem{A: c.a, B: c.b, State: ResultNotEqual}, "")
		assert.Equal(t, strings.Join(c.lines, "\n")+"\n", f.builder.String(), fmt.Sprintf(`case "%d"`, i))
	}
}

func TestFormatter_FormatStrings(t *testing.T) {
	t.Parallel()
	cases := []struct{ a, b, result string }{
		{"", "", ""},
		{"foo\n", "", "- foo\n- \n"},
		{"foo\n", "bar\n", "- foo\n+ bar\n\n"},
		{"foo", "foo", ""},
		{"foo", "bar", "- foo\n+ bar\n"},
		{"abc\nfoo", "abc\nbar", "  abc\n- foo\n+ bar\n"},
		{"abc\nfoo\nabc", "abc\nbar\nabc", "  abc\n- foo\n+ bar\n  abc\n"},
	}
	for i, c := range cases {
		f := newFormatter()
		f.formatStrings(c.a, c.b, "")
		assert.Equal(t, c.result, f.builder.String(), fmt.Sprintf(`case "%d"`, i))
	}
}

func TestFormatter_FormatStrings_MaxEqualLines(t *testing.T) {
	t.Parallel()
	A := `
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

	B := `
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

	f := newFormatter()
	f.formatStrings(strings.Trim(A, "\n"), strings.Trim(B, "\n"), "")
	assert.Equal(t, strings.TrimLeft(expected, "\n"), f.builder.String())
}
