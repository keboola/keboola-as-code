package links

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeLinksMatchId(t *testing.T) {
	t.Parallel()
	cases := []struct{ input, expected string }{
		{input: "", expected: ""},
		{input: "abc", expected: ""},
		{input: "{}", expected: ""},
		{input: "{{}}", expected: ""},
		{input: "{{{abc}}}", expected: ""},
		{input: "{{abc/def}}", expected: ""},
		{input: "{{1}}", expected: "1"},
		{input: "{{123}}", expected: "123"},
		{input: "{{a}}", expected: "a"},
		{input: "{{abc}}", expected: "abc"},
		{input: "{{A}}", expected: "A"},
		{input: "{{ABC}}", expected: "ABC"},
		{input: "  {{ABC}}  \n", expected: "ABC"},
	}
	m := NewMapper(nil, model.MapperContext{Naming: model.DefaultNaming()})
	for i, c := range cases {
		assert.Equal(t, c.expected, m.matchId(c.input), fmt.Sprintf(`Case "%d"`, i))
	}
}

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
	m := NewMapper(nil, model.MapperContext{Naming: model.DefaultNaming()})
	for i, c := range cases {
		assert.Equal(t, c.expected, m.matchPath(c.input, `keboola.snowflake-transformation`), fmt.Sprintf(`Case "%d"`, i))
	}
}

func TestSharedCodeLinksFormatId(t *testing.T) {
	t.Parallel()
	m := NewMapper(nil, model.MapperContext{Naming: model.DefaultNaming()})
	assert.Equal(t, `{{12345}}`, m.formatId(`12345`))
}

func TestSharedCodeLinksFormatPath(t *testing.T) {
	t.Parallel()
	m := NewMapper(nil, model.MapperContext{Naming: model.DefaultNaming()})

	assert.Equal(t, `-- {{:foo/bar}}`, m.formatPath(`foo/bar`, `keboola.snowflake-transformation`))
	assert.Equal(t, `# {{:foo/bar}}`, m.formatPath(`foo/bar`, `keboola.python-transformation-v2`))
}
