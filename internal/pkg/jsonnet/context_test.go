package jsonnet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-jsonnet"
	"github.com/google/go-jsonnet/ast"
	"github.com/stretchr/testify/assert"
)

func TestVmContext_Nil(t *testing.T) {
	t.Parallel()
	output, err := Evaluate(`{foo: "bar"}`, nil)
	assert.NoError(t, err)
	assert.Equal(t, "{\n  \"foo\": \"bar\"\n}\n", output)
}

func TestVmContext_Empty(t *testing.T) {
	t.Parallel()
	ctx := NewContext()
	output, err := Evaluate(`{foo: "bar"}`, ctx)
	assert.NoError(t, err)
	assert.Equal(t, "{\n  \"foo\": \"bar\"\n}\n", output)
}

func TestVmContext_Complex(t *testing.T) {
	t.Parallel()
	ctx := NewContext()
	ctx.ExtVar("var1", "value1")
	ctx.ExtVar("var2", "value2")
	ctx.NativeFunction(&jsonnet.NativeFunction{
		Name:   `func1`,
		Params: ast.Identifiers{"param1", "param2"},
		Func: func(params []interface{}) (interface{}, error) {
			return fmt.Sprintf("---%s---%s---", params[0], params[1]), nil
		},
	})
	ctx.NativeFunctionWithAlias(&jsonnet.NativeFunction{
		Name:   `func2`,
		Params: ast.Identifiers{"param1", "param2"},
		Func: func(params []interface{}) (interface{}, error) {
			return fmt.Sprintf("***%s***%s***", params[0], params[1]), nil
		},
	})

	// String
	ctx.GlobalBinding(`alias1`, &ast.LiteralString{Value: "aliasValue", Kind: ast.StringDouble})

	// 1+2
	ctx.GlobalBinding(`alias2`, &ast.Binary{Left: &ast.LiteralNumber{OriginalString: "1"}, Right: &ast.LiteralNumber{OriginalString: "2"}, Op: ast.BopPlus})

	code := `
{
  var1: std.extVar("var1"),
  var2: std.extVar("var2"),
  func1: std.native("func1")("param1", "param2"),
  func2: std.native("func2")("param1", "param2"),
  alias1: alias1,
  alias2: alias2,
  func2Alias: func2("param1", "param2"),
}
`

	expected := `
{
  "alias1": "aliasValue",
  "alias2": 3,
  "func1": "---param1---param2---",
  "func2": "***param1***param2***",
  "func2Alias": "***param1***param2***",
  "var1": "value1",
  "var2": "value2"
}
`

	output, err := Evaluate(code, ctx)
	assert.NoError(t, err)
	assert.Equal(t, strings.TrimLeft(expected, "\n"), output)
}

func TestVmContext_VariablesTypes(t *testing.T) {
	t.Parallel()

	ctx := NewContext()
	ctx.ExtVar("myNull", nil)
	ctx.ExtVar("myBool", true)
	ctx.ExtVar("myString", "myString")
	ctx.ExtVar("myFloat", 12.34)
	ctx.ExtVar("myInt", 789)

	code := `
{ 
  "null": std.extVar("myNull"),
  "bool": std.extVar("myBool"),
  "string": std.extVar("myString"),
  "float": std.extVar("myFloat"),
  "int": std.extVar("myInt"),
}`

	expected := `
{
  "bool": true,
  "float": 12.34,
  "int": 789,
  "null": null,
  "string": "myString"
}
`

	output, err := Evaluate(code, ctx)
	assert.NoError(t, err)
	assert.Equal(t, strings.TrimLeft(expected, "\n"), output)
}

func TestVmContext_Notifier(t *testing.T) {
	t.Parallel()
	notifier := &testNotifier{}
	ctx := NewContext()
	ctx.NativeFunctionWithAlias(&NativeFunction{
		Name:   `Decorate`,
		Params: ast.Identifiers{"str"},
		Func: func(params []interface{}) (interface{}, error) {
			return fmt.Sprintf("~%s~", params[0].(string)), nil
		},
	})
	ctx.NotifierFactory(func(context.Context) jsonnet.Notifier {
		return notifier
	})

	// Jsonnet code
	code := `
local Person(name='Alice') = {
  name: if true then Decorate(name) else null,
};
local Do() = {
  person1: Person(),
  person2: Person('Bob'),
  other: [Person('Foo'), Person('Bar')],
};
Do()
`

	// Output Json
	expected := `{
  "other": [
    {
      "name": "~Foo~"
    },
    {
      "name": "~Bar~"
    }
  ],
  "person1": {
    "name": "~Alice~"
  },
  "person2": {
    "name": "~Bob~"
  }
}
`

	// Notified values
	expectedNotifications := []generatedValue{
		{fnName: "Decorate", args: []interface{}{"Foo"}, value: "~Foo~", steps: []interface{}{jsonnet.ObjectFieldStep{Field: "other"}, jsonnet.ArrayIndexStep{Index: 0}, jsonnet.ObjectFieldStep{Field: "name"}}},
		{fnName: "Decorate", args: []interface{}{"Bar"}, value: "~Bar~", steps: []interface{}{jsonnet.ObjectFieldStep{Field: "other"}, jsonnet.ArrayIndexStep{Index: 1}, jsonnet.ObjectFieldStep{Field: "name"}}},
		{fnName: "Decorate", args: []interface{}{"Alice"}, value: "~Alice~", steps: []interface{}{jsonnet.ObjectFieldStep{Field: "person1"}, jsonnet.ObjectFieldStep{Field: "name"}}},
		{fnName: "Decorate", args: []interface{}{"Bob"}, value: "~Bob~", steps: []interface{}{jsonnet.ObjectFieldStep{Field: "person2"}, jsonnet.ObjectFieldStep{Field: "name"}}},
	}

	actual, err := Evaluate(code, ctx)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
	assert.Equal(t, expectedNotifications, notifier.values)
}

type testNotifier struct {
	values []generatedValue
}

type generatedValue struct {
	fnName string
	args   []interface{}
	value  interface{}
	steps  []interface{}
}

func (n *testNotifier) OnGeneratedValue(fnName string, args []interface{}, value interface{}, steps []interface{}) {
	n.values = append(n.values, generatedValue{
		fnName: fnName,
		args:   args,
		value:  value,
		steps:  steps,
	})
}
