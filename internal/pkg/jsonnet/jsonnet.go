package jsonnet

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/google/go-jsonnet"
	"github.com/google/go-jsonnet/ast"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/third_party/jsonnet/lib/formatter"
	"github.com/keboola/keboola-as-code/third_party/jsonnet/lib/parser"
	"github.com/keboola/keboola-as-code/third_party/jsonnet/lib/program"
)

type VariablesValues map[string]interface{}

func Evaluate(code string, vars VariablesValues) (jsonOut string, err error) {
	node, err := ToAst(code)
	if err != nil {
		return "", err
	}
	return EvaluateAst(node, vars)
}

func MustEvaluate(code string, vars VariablesValues) (jsonOut string) {
	jsonOut, err := Evaluate(code, vars)
	if err != nil {
		panic(err)
	}
	return jsonOut
}

func EvaluateAst(input ast.Node, vars VariablesValues) (jsonOut string, err error) {
	// Pre-process
	node := ast.Clone(input)
	if err := program.PreprocessAst(&node); err != nil {
		return "", err
	}

	// Evaluate
	vm := jsonnet.MakeVM()
	registerVariables(vm, vars)
	jsonContent, err := vm.Evaluate(node)
	if err != nil {
		return "", fmt.Errorf(`jsonnet error: %w`, err)
	}

	// Format (go-jsonnet library use 3 space indent)
	var out bytes.Buffer
	if err := json.Indent(&out, []byte(jsonContent), "", "  "); err != nil {
		return "", err
	}
	return out.String(), nil
}

func MustEvaluateAst(input ast.Node, vars VariablesValues) (jsonOut string) {
	jsonOut, err := EvaluateAst(input, vars)
	if err != nil {
		panic(err)
	}
	return jsonOut
}

func Format(code string) (string, error) {
	return formatter.Format(``, code, DefaultOptions())
}

func MustFormat(code string) string {
	out, err := Format(code)
	if err != nil {
		panic(err)
	}
	return out
}

func FormatAst(node ast.Node) (string, error) {
	return formatter.FormatAst(node, DefaultOptions())
}

func MustFormatAst(node ast.Node) string {
	out, err := FormatAst(node)
	if err != nil {
		panic(err)
	}
	return out
}

func ToAst(code string) (ast.Node, error) {
	node, _, err := parser.SnippetToRawAST(``, ``, code)
	if err != nil {
		return nil, fmt.Errorf(`cannot parse jsonnet: %w`, err)
	}
	return node, nil
}

func MustToAst(code string) ast.Node {
	node, err := ToAst(code)
	if err != nil {
		panic(err)
	}
	return node
}

func DefaultOptions() formatter.Options {
	return formatter.Options{
		Indent:           2,
		MaxBlankLines:    2,
		PrettyFieldNames: true,
		PadArrays:        true,
		PadObjects:       true,
		SortImports:      true,
		StringStyle:      formatter.StringStyleDouble,
		CommentStyle:     formatter.CommentStyleSlash,
	}
}

func registerVariables(vm *jsonnet.VM, vars VariablesValues) {
	for k, v := range vars {
		if v == nil {
			vm.ExtNode(k, &ast.LiteralNull{})
			continue
		}

		switch v := v.(type) {
		case bool:
			vm.ExtNode(k, &ast.LiteralBoolean{Value: v})
		case int:
			vm.ExtNode(k, &ast.LiteralNumber{OriginalString: cast.ToString(v)})
		case int32:
			vm.ExtNode(k, &ast.LiteralNumber{OriginalString: cast.ToString(v)})
		case int64:
			vm.ExtNode(k, &ast.LiteralNumber{OriginalString: cast.ToString(v)})
		case float32:
			vm.ExtNode(k, &ast.LiteralNumber{OriginalString: cast.ToString(v)})
		case float64:
			vm.ExtNode(k, &ast.LiteralNumber{OriginalString: cast.ToString(v)})
		default:
			vm.ExtVar(k, cast.ToString(v))
		}
	}
}
