package links

import (
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
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
	idUtils := newIDUtils()
	for i, c := range cases {
		assert.Equal(t, keboola.RowID(c.expected), idUtils.match(c.input), `Case "%d"`, i)
	}
}

func TestSharedCodeLinksFormatId(t *testing.T) {
	t.Parallel()
	idUtils := newIDUtils()
	assert.Equal(t, `{{12345}}`, idUtils.format(`12345`))
}
