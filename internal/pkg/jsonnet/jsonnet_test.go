package jsonnet

import (
	"testing"

	"github.com/google/go-jsonnet/ast"
	"github.com/stretchr/testify/assert"
)

func TestEvaluate(t *testing.T) {
	t.Parallel()
	code := `{ foo: "bar" }`
	json, err := Evaluate(code)
	assert.NoError(t, err)
	assert.Equal(t, "{\n  \"foo\": \"bar\"\n}\n", json)
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
	json, err := EvaluateAst(astNode)
	assert.NoError(t, err)
	assert.Equal(t, "{\n  \"foo\": \"bar\"\n}\n", json)
}

func TestFormat(t *testing.T) {
	t.Parallel()
	code := `{"foo":"bar"}`
	jsonNet, err := Format(code)
	assert.NoError(t, err)
	assert.Equal(t, "{ foo: \"bar\" }\n", jsonNet)
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
	jsonNet, err := FormatAst(astNode)
	assert.NoError(t, err)
	assert.Equal(t, "{ foo: \"bar\" }\n", jsonNet)
}

func TestToAst(t *testing.T) {
	t.Parallel()
	code := `{ foo: "bar" }`
	astNode, err := ToAst(code)
	assert.NoError(t, err)
	assert.NotEmpty(t, astNode)
}
