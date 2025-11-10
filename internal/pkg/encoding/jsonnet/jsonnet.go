package jsonnet

import (
	"bytes"
	"encoding/json"

	"github.com/google/go-jsonnet"
	"github.com/google/go-jsonnet/ast"
	"github.com/google/go-jsonnet/formatter"
	"github.com/google/go-jsonnet/parser"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func Evaluate(code string, ctx *Context) (jsonOut string, err error) {
	node, err := ToAst(code, ctx.FilePath())
	if err != nil {
		return "", err
	}
	return EvaluateAst(node, ctx)
}

func MustEvaluate(code string, ctx *Context) (jsonOut string) {
	jsonOut, err := Evaluate(code, ctx)
	if err != nil {
		panic(err)
	}
	return jsonOut
}

func EvaluateAst(input ast.Node, ctx *Context) (jsonOut string, err error) {
	// Create VM
	vm := jsonnet.MakeVM()
	vm.Importer(NewNopImporter()) // default
	ctx.registerTo(vm)

	// Pre-process
	node := ast.Clone(input)
	if err := parser.PreprocessAst(&node, vm.GlobalVars()...); err != nil {
		return "", err
	}

	// Evaluate
	jsonContent, err := vm.Evaluate(node)
	if err != nil {
		return "", errors.Errorf(`jsonnet error: %w`, err)
	}

	// Format (go-jsonnet library use 3 space indent)
	var out bytes.Buffer
	if ctx.Pretty() {
		if err := json.Indent(&out, []byte(jsonContent), "", "  "); err != nil {
			return "", err
		}
	} else {
		if err := json.Compact(&out, []byte(jsonContent)); err != nil {
			return "", err
		}
	}

	return out.String(), nil
}

func MustEvaluateAst(input ast.Node, ctx *Context) (jsonOut string) {
	jsonOut, err := EvaluateAst(input, ctx)
	if err != nil {
		panic(err)
	}
	return jsonOut
}

func Format(code string) (string, error) {
	node, err := ToAst(code, "")
	if err != nil {
		return "", err
	}
	return FormatNode(node)
}

func FormatNode(node ast.Node) (string, error) {
	node = ast.Clone(node)
	ReplacePlaceholdersRecursive(node)
	return formatter.FormatNode(node, nil, DefaultOptions())
}

func ToAst(code, fileName string) (ast.Node, error) {
	node, _, err := parser.SnippetToRawAST(code, fileName)
	if err != nil {
		return nil, errors.Errorf(`cannot parse jsonnet: %w`, err)
	}
	return node, nil
}

func MustToAst(code string, fileName string) ast.Node {
	node, err := ToAst(code, fileName)
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

func Alias(name string) *ast.Apply {
	return &ast.Apply{
		Target: &ast.Index{
			Target: &ast.Var{Id: "std"},
			Index:  &ast.LiteralString{Value: "native"},
		},
		Arguments: ast.Arguments{Positional: []ast.CommaSeparatedExpr{{Expr: &ast.LiteralString{Value: name}}}},
	}
}
