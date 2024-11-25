package links

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSharedCodeLinksMatchPath(t *testing.T) {
	t.Parallel()
	cases := []struct{ input, expected string }{
		{input: "", expected: ""},
		{input: "abc", expected: ""},
		{input: "{}", expected: ""},
		{input: "{{}}", expected: ""},
		{input: "{:}", expected: ""},
		{input: "{{:}}", expected: ""},
		{input: "{{{:abc}}}", expected: ""},
		{input: "{{:1}}", expected: "1"},
		{input: "{{:123}}", expected: "123"},
		{input: "{{:a}}", expected: "a"},
		{input: "{{:abc}}", expected: "abc"},
		{input: "{{:A}}", expected: "A"},
		{input: "{{:ABC}}", expected: "ABC"},
		{input: "  {{:ABC}}  \n", expected: "ABC"},
		{input: "{{:codes/my-code}}", expected: "codes/my-code"},
		{input: "-- {{:codes/my-code}}", expected: "codes/my-code"}, // SQL comment
	}
	pathUtils := newPathUtils()
	for i, c := range cases {
		assert.Equal(t, c.expected, pathUtils.match(c.input, `keboola.snowflake-transformation`), `Case "%d"`, i)
	}
}

func TestSharedCodeLinksFormatPath(t *testing.T) {
	t.Parallel()
	pathUtils := newPathUtils()
	assert.Equal(t, `-- {{:foo/bar}}`, pathUtils.format(`foo/bar`, `keboola.snowflake-transformation`))
	assert.Equal(t, `# {{:foo/bar}}`, pathUtils.format(`foo/bar`, `keboola.python-transformation-v2`))
}
