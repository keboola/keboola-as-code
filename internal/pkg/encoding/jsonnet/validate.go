package jsonnet

import (
	"github.com/google/go-jsonnet/ast"
	"github.com/google/go-jsonnet/parser"
)

func (ctx *Context) Validate(code, fileName string) error {
	// get global vars
	vars := make([]ast.Identifier, 0, len(ctx.globalBinding))
	for k := range ctx.globalBinding {
		vars = append(vars, k)
	}

	_, err := parser.SnippetToAst(code, fileName, vars...)
	return err
}
