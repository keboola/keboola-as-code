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

func Evaluate(code string) (jsonOut string, err error) {
	node, err := ToAst(code)
	if err != nil {
		return "", err
	}
	return EvaluateAst(node)
}

func EvaluateAst(input ast.Node) (jsonOut string, err error) {
	// Pre-process
	node := ast.Clone(input)
	if err := program.PreprocessAst(&node); err != nil {
		return "", err
	}

	// Evaluate
	jsonContent, err := jsonnet.MakeVM().Evaluate(node)
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
	return node, err
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
