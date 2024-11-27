package jsonnet

import (
	"testing"

	"github.com/google/go-jsonnet/ast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluate(t *testing.T) {
	t.Parallel()
	code := `{ foo: "bar" }`
	json, err := Evaluate(code, nil)
	require.NoError(t, err)
	assert.JSONEq(t, `{"foo":"bar"}`, json)
}

func TestEvaluateAst(t *testing.T) {
	t.Parallel()
	astNode := &ast.Object{
		Fields: ast.ObjectFields{
			{
				Kind:  ast.ObjectFieldStr,
				Hide:  ast.ObjectFieldInherit,
				Expr1: &ast.LiteralString{Value: "foo"},
				Expr2: &ast.LiteralString{Value: "bar"},
			},
		},
	}
	json, err := EvaluateAst(astNode, nil)
	require.NoError(t, err)
	assert.JSONEq(t, `{"foo":"bar"}`, json)
}

func TestFormat(t *testing.T) {
	t.Parallel()
	code := `{"foo":"bar"}`
	jsonnetStr, err := Format(code)
	require.NoError(t, err)
	assert.Equal(t, "{ foo: \"bar\" }\n", jsonnetStr) //nolint: testifylint
}

func TestFormatAst(t *testing.T) {
	t.Parallel()
	astNode := &ast.Object{
		Fields: ast.ObjectFields{
			{
				Kind:  ast.ObjectFieldStr,
				Hide:  ast.ObjectFieldInherit,
				Expr1: &ast.LiteralString{Value: "foo"},
				Expr2: &ast.LiteralString{Value: "bar"},
			},
		},
	}
	jsonnetStr := FormatAst(astNode)
	assert.Equal(t, "{ foo: \"bar\" }\n", jsonnetStr) //nolint: testifylint
}

func TestToAst(t *testing.T) {
	t.Parallel()
	code := `{ foo: "bar" }`
	astNode, err := ToAst(code, "")
	require.NoError(t, err)
	assert.NotEmpty(t, astNode)
}
