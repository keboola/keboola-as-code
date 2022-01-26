package jsonnet

import (
	"testing"

	"github.com/google/go-jsonnet/ast"
	"github.com/stretchr/testify/assert"
)

func TestEvaluate(t *testing.T) {
	t.Parallel()
	code := `{ foo: "bar" }`
	json, err := Evaluate(code, nil)
	assert.NoError(t, err)
	assert.Equal(t, "{\n  \"foo\": \"bar\"\n}\n", json)
}

func TestEvaluate_Variables(t *testing.T) {
	t.Parallel()

	code := `{ 
  "null": std.extVar("myNull"),
  "bool": std.extVar("myBool"),
  "string": std.extVar("myString"),
  "float": std.extVar("myFloat"),
  "int": std.extVar("myInt"),
}`

	expected := `{
  "bool": true,
  "float": 12.34,
  "int": 789,
  "null": null,
  "string": "myString"
}
`

	variables := VariablesValues{
		"myNull":   nil,
		"myBool":   true,
		"myString": "myString",
		"myFloat":  12.34,
		"myInt":    789,
	}

	json, err := Evaluate(code, variables)
	assert.NoError(t, err)
	assert.Equal(t, expected, json)
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
