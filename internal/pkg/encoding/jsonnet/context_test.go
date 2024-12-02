package jsonnet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-jsonnet"
	"github.com/google/go-jsonnet/ast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVmContext_Nil(t *testing.T) {
	t.Parallel()
	output, err := Evaluate(`{foo: "bar"}`, nil)
	require.NoError(t, err)
	assert.JSONEq(t, `{"foo":"bar"}`, output)
}

func TestVmContext_Empty(t *testing.T) {
	t.Parallel()
	ctx := NewContext()
	output, err := Evaluate(`{foo: "bar"}`, ctx)
	require.NoError(t, err)
	assert.JSONEq(t, `{"foo":"bar"}`, output)
}

func TestVmContext_Pretty_False(t *testing.T) {
	t.Parallel()
	ctx := NewContext().WithPretty(false)
	output, err := Evaluate(`{foo: "bar"}`, ctx)
	require.NoError(t, err)
	assert.JSONEq(t, `{"foo":"bar"}`, output)
}

func TestVmContext_Complex(t *testing.T) {
	t.Parallel()
	ctx := NewContext()
	ctx.ExtVar("var1", "value1")
	ctx.ExtVar("var2", "value2")
	ctx.NativeFunction(&jsonnet.NativeFunction{
		Name:   `func1`,
		Params: ast.Identifiers{"param1", "param2"},
		Func: func(params []any) (any, error) {
			return fmt.Sprintf("---%s---%s---", params[0], params[1]), nil
		},
	})
	ctx.NativeFunctionWithAlias(&jsonnet.NativeFunction{
		Name:   `func2`,
		Params: ast.Identifiers{"param1", "param2"},
		Func: func(params []any) (any, error) {
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
	require.NoError(t, err)
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
	require.NoError(t, err)
	assert.Equal(t, strings.TrimLeft(expected, "\n"), output)
}

func TestVmContext_ValueToLiteral_MapArray(t *testing.T) {
	t.Parallel()

	result := ValueToLiteral(map[string]any{"one": map[string]any{"two": 2, "three": "four"}, "five": []any{"six", 7, true}})

	vm := jsonnet.MakeVM()
	vm.Importer(NewNopImporter())
	ctx := NewContext()
	ctx.registerTo(vm)

	jsonContent, err := vm.Evaluate(result)
	require.NoError(t, err)

	var evaluatedResult bytes.Buffer
	err = json.Indent(&evaluatedResult, []byte(jsonContent), "", "  ")
	require.NoError(t, err)

	expected := `{
  "five": [
    "six",
    7,
    true
  ],
  "one": {
    "three": "four",
    "two": 2
  }
}
`
	assert.Equal(t, expected, evaluatedResult.String())
}

func TestVmContext_Notifier(t *testing.T) {
	t.Parallel()
	ctx := NewContext()

	// Test notifier
	notifier := &testNotifier{}
	ctx.NotifierFactory(func(context.Context) jsonnet.Notifier {
		return notifier
	})

	// Function "decorate" wraps string value with "~".
	ctx.NativeFunction(&NativeFunction{
		Name:   `decorate`,
		Params: ast.Identifiers{"str"},
		Func: func(params []any) (any, error) {
			return fmt.Sprintf("~%s~", params[0].(string)), nil
		},
	})

	// Function "keyValueObject" converts key and value to object.
	ctx.NativeFunction(&NativeFunction{
		Name:   `keyValueObject`,
		Params: ast.Identifiers{"key", "value"},
		Func: func(params []any) (any, error) {
			return map[string]any{params[0].(string): params[1].(string)}, nil
		},
	})

	// Input Jsonnet code.
	code := `
local Person(name='Alice') = {
  name: if true then std.native('decorate')(name) else null,
};
local Do() = {
  myObject: {
    mergedObject: 
		std.native('keyValueObject')("A", "AAA") +
		std.native('keyValueObject')("B", "BBB") +
		{
			"sub": std.native('keyValueObject')("C", "CCC")
		}
  },
  person1: Person(),
  person2: Person('Bob'),
  other: [Person('Foo'), Person('Bar')],
};
Do()
`

	// Expected output Json.
	expected := `{
  "myObject": {
    "mergedObject": {
      "A": "AAA",
      "B": "BBB",
      "sub": {
        "C": "CCC"
      }
    }
  },
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

	// Notified values:
	expectedNotifications := []generatedValue{
		// Objects merging
		{
			fnName:  "keyValueObject",
			args:    []any{"C", "CCC"},
			partial: false,
			partialValue: map[string]any{
				"C": "CCC",
			},
			finalValue: map[string]any{
				"C": "CCC",
			},
			steps: []any{
				jsonnet.ObjectFieldStep{Field: "myObject"},
				jsonnet.ObjectFieldStep{Field: "mergedObject"},
				jsonnet.ObjectFieldStep{Field: "sub"},
			},
		},
		{
			fnName:  "keyValueObject",
			args:    []any{"A", "AAA"},
			partial: true,
			partialValue: map[string]any{
				"A": "AAA",
			},
			finalValue: map[string]any{
				"A": "AAA",
				"B": "BBB",
				"sub": map[string]any{
					"C": "CCC",
				},
			},
			steps: []any{
				jsonnet.ObjectFieldStep{Field: "myObject"},
				jsonnet.ObjectFieldStep{Field: "mergedObject"},
			},
		},
		{
			fnName:  "keyValueObject",
			args:    []any{"B", "BBB"},
			partial: true,
			partialValue: map[string]any{
				"B": "BBB",
			},
			finalValue: map[string]any{
				"A": "AAA",
				"B": "BBB",
				"sub": map[string]any{
					"C": "CCC",
				},
			},
			steps: []any{
				jsonnet.ObjectFieldStep{Field: "myObject"},
				jsonnet.ObjectFieldStep{Field: "mergedObject"},
			},
		},
		// Simple usage
		{
			fnName:       "decorate",
			args:         []any{"Foo"},
			partial:      false,
			partialValue: "~Foo~",
			finalValue:   "~Foo~",
			steps: []any{
				jsonnet.ObjectFieldStep{Field: "other"},
				jsonnet.ArrayIndexStep{Index: 0},
				jsonnet.ObjectFieldStep{Field: "name"},
			},
		},
		{
			fnName:       "decorate",
			args:         []any{"Bar"},
			partial:      false,
			partialValue: "~Bar~",
			finalValue:   "~Bar~",
			steps: []any{
				jsonnet.ObjectFieldStep{Field: "other"},
				jsonnet.ArrayIndexStep{Index: 1},
				jsonnet.ObjectFieldStep{Field: "name"},
			},
		},
		{
			fnName:       "decorate",
			args:         []any{"Alice"},
			partial:      false,
			partialValue: "~Alice~",
			finalValue:   "~Alice~",
			steps: []any{
				jsonnet.ObjectFieldStep{Field: "person1"},
				jsonnet.ObjectFieldStep{Field: "name"},
			},
		},
		{
			fnName:       "decorate",
			args:         []any{"Bob"},
			partial:      false,
			partialValue: "~Bob~",
			finalValue:   "~Bob~",
			steps: []any{
				jsonnet.ObjectFieldStep{Field: "person2"},
				jsonnet.ObjectFieldStep{Field: "name"},
			},
		},
	}

	// Evaluate and assert
	actual, err := Evaluate(code, ctx)
	require.NoError(t, err)
	assert.Equal(t, expected, actual)
	assert.Equal(t, expectedNotifications, notifier.values)
}

type testNotifier struct {
	values []generatedValue
}

type generatedValue struct {
	fnName       string
	args         []any
	partial      bool
	partialValue any
	finalValue   any
	steps        []any
}

func (n *testNotifier) OnGeneratedValue(fnName string, args []any, partial bool, partialValue, finalValue any, steps []any) {
	n.values = append(n.values, generatedValue{
		fnName:       fnName,
		args:         args,
		partial:      partial,
		partialValue: partialValue,
		finalValue:   finalValue,
		steps:        steps,
	})
}
