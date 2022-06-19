package links

import (
	"fmt"
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
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
	idUtils := newIdUtils()
	for i, c := range cases {
		assert.Equal(t, storageapi.RowID(c.expected), idUtils.match(c.input), fmt.Sprintf(`Case "%d"`, i))
	}
}

func TestSharedCodeLinksFormatId(t *testing.T) {
	t.Parallel()
	idUtils := newIdUtils()
	assert.Equal(t, `{{12345}}`, idUtils.format(`12345`))
}
