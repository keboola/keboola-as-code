package jsonnet

import (
	"strings"
	"testing"

	"github.com/google/go-jsonnet/ast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVisitAst(t *testing.T) {
	t.Parallel()
	code := `
{
  values: [
    {
      key: {
        int: 123,
        string: "str",
        op: 1 + 2,
      }
    }
  ]
}
`

	expected := `
{
  values: [
    {
      key: {
        int: "replaced type",
        string: "modified-str",
        op: 1001 + 1002,
      },
    },
  ],
}
`

	// Visit & modify
	node, err := ToAst(code, "")
	require.NoError(t, err)
	VisitAst(&node, func(nodePtr *ast.Node) {
		switch v := (*nodePtr).(type) {
		case *ast.LiteralString:
			v.Value = "modified-" + v.Value
		case *ast.LiteralNumber:
			if v.OriginalString == "123" {
				*nodePtr = &ast.LiteralString{Value: "replaced type", Kind: ast.StringDouble}
			} else {
				v.OriginalString = "100" + v.OriginalString
			}
		}
	})

	assert.Equal(t, strings.TrimLeft(expected, "\n"), FormatAst(node))
}
