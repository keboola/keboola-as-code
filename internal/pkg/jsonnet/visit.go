package jsonnet

import (
	"github.com/google/go-jsonnet/ast"

	"github.com/keboola/keboola-as-code/third_party/jsonnet/lib/pass"
)

type VisitFunc func(nodePtr *ast.Node)

type visitor struct {
	pass.Base
	callback VisitFunc
}

// Visit replaces ApplyBrace with Binary node.
func (v *visitor) Visit(p pass.ASTPass, node *ast.Node, ctx pass.Context) {
	v.callback(node)
	v.Base.Visit(p, node, ctx)
}

func VisitAst(node *ast.Node, callback VisitFunc) {
	v := &visitor{callback: callback}
	v.Visit(v, node, nil)
}
