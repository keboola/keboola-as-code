package jsonnet

import (
	"github.com/google/go-jsonnet/ast"
	"github.com/google/go-jsonnet/parser"
)

func (c *Context) Validate(code string) error {
	// get global vars
	vars := make([]ast.Identifier, 0, len(c.globalBinding))
	for k := range c.globalBinding {
		vars = append(vars, k)
	}

	_, err := parser.SnippetToAst(code, c.FilePath(), vars...)
	return err
}
