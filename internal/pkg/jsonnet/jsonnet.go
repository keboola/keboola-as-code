package jsonnet

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/google/go-jsonnet"
	"github.com/google/go-jsonnet/ast"

	"github.com/keboola/keboola-as-code/third_party/jsonnet/lib/formatter"
	"github.com/keboola/keboola-as-code/third_party/jsonnet/lib/parser"
	"github.com/keboola/keboola-as-code/third_party/jsonnet/lib/program"
)

func Evaluate(code string, ctx *Context) (jsonOut string, err error) {
	node, err := ToAst(code)
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
	if err := program.PreprocessAst(&node); err != nil {
		return "", err
	}

	// Create VM
	vm := jsonnet.MakeVM()
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
