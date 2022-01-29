package jsonnet

import (
	"testing"

	"github.com/google/go-jsonnet/ast"
	"github.com/stretchr/testify/assert"
)

func TestConfigIdPlaceholder(t *testing.T) {
	t.Parallel()
	assert.Equal(t, `<<~~func:ConfigId:["my-config-id"]~~>>`, ConfigIdPlaceholder("my-config-id"))
}

func TestStripIdPlaceholder_NotFound(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "", StripIdPlaceholder(""))
	assert.Equal(t, "foo bar", StripIdPlaceholder("foo bar"))
}

func TestStripIdPlaceholder_ConfigId(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "my-config-id", StripIdPlaceholder(ConfigIdPlaceholder("my-config-id")))
}

func TestStripIdPlaceholder_ConfigRowId(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "my-config-row-id", StripIdPlaceholder(ConfigIdPlaceholder("my-config-row-id")))
}

func TestConfigRowIdPlaceholder(t *testing.T) {
	t.Parallel()
	t.Parallel()
	assert.Equal(t, `<<~~func:ConfigRowId:["my-config-row-id"]~~>>`, ConfigRowIdPlaceholder("my-config-row-id"))
}

func TestReplaceFuncCallPlaceholders(t *testing.T) {
	t.Parallel()

	cases := []struct{ input, expected string }{
		{
			input:    ``,
			expected: `""`,
		},
		{
			input:    `<<~~func:FuncName:["foo", "bar", 123]~~>>`,
			expected: `FuncName("foo", "bar", 123)`,
		},
		{
			input:    `in.c-keboola-ex-aws-s3-<<~~func:ConfigId:["om-default-bucket"]~~>>.table`,
			expected: `"in.c-keboola-ex-aws-s3-" + ConfigId("om-default-bucket") + ".table"`,
		},
		{
			input:    `<<~~func:ConfigId:["my-id"]~~>>`,
			expected: `ConfigId("my-id")`,
		},
		{
			input:    `  <<~~func:ConfigId:["my-id"]~~>>  `,
			expected: `"  " + ConfigId("my-id") + "  "`,
		},
		{
			input:    `before <<~~func:ConfigId:["my-id"]~~>>`,
			expected: `"before " + ConfigId("my-id")`,
		},
		{
			input:    `<<~~func:ConfigId:["my-id"]~~>> after`,
			expected: `ConfigId("my-id") + " after"`,
		},
		{
			input:    `before <<~~func:ConfigId:["my-id"]~~>> middle <<~~func:ConfigId:["my-id"]~~>> end`,
			expected: `"before " + ConfigId("my-id") + " middle " + ConfigId("my-id") + " end"`,
		},
	}

	for i, c := range cases {
		replaced, err := FormatAst(ReplacePlaceholders(&ast.LiteralString{Value: c.input}))
		assert.NoError(t, err, i)
		assert.Equal(t, c.expected+"\n", replaced, i)
	}
}
