package jsonnet

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/google/go-jsonnet"
	"github.com/google/go-jsonnet/ast"
	"github.com/google/go-jsonnet/formatter"
	"github.com/google/go-jsonnet/parser"
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
	// Pre-process
	node := ast.Clone(ctx.wrapAst(input))
	if err := parser.PreprocessAst(&node); err != nil {
		return "", err
	}

	// Create VM
	vm := jsonnet.MakeVM()
	vm.Importer(NewNopImporter()) // default
	ctx.registerTo(vm)

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
	return FormatAst(node), nil
}

func FormatAst(node ast.Node) string {
	node = ast.Clone(node)
	ReplacePlaceholdersRecursive(node)
	return formatter.FormatAst(node, nil, DefaultOptions())
}

func ToAst(code, fileName string) (ast.Node, error) {
	node, _, err := parser.SnippetToRawAST(code, fileName)
	if err != nil {
		return nil, fmt.Errorf(`cannot parse jsonnet: %w`, err)
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
