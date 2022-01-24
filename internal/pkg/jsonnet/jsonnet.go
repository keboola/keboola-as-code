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

func Evaluate(code string, variables map[string]interface{}) (jsonOut string, err error) {
	node, err := ToAst(code)
	if err != nil {
		return "", err
	}
	return EvaluateAst(node, variables)
}

func EvaluateAst(input ast.Node, variables map[string]interface{}) (jsonOut string, err error) {
	// Pre-process
	node := ast.Clone(input)
	if err := program.PreprocessAst(&node); err != nil {
		return "", err
	}

	// Create VM
	vm := jsonnet.MakeVM()

	// Set variables
	// https://jsonnet.org/learning/tutorial.html#parameterize-entire-config
	registerVariables(vm, variables)

	// Evaluate
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

func Format(code string) (string, error) {
	return formatter.Format(``, code, DefaultOptions())
}

func FormatAst(node ast.Node) (string, error) {
	return formatter.FormatAst(node, DefaultOptions())
}

func ToAst(code string) (ast.Node, error) {
	node, _, err := parser.SnippetToRawAST(``, ``, code)
	if err != nil {
		return nil, fmt.Errorf(`cannot parse jsonnet: %w`, err)
	}
	return node, nil
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

func registerVariables(vm *jsonnet.VM, variables map[string]interface{}) {
	for k, v := range variables {
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
