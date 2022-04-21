package jsonnet

import (
	"fmt"

	"github.com/google/go-jsonnet"
	"github.com/google/go-jsonnet/ast"
	"github.com/spf13/cast"
)

type Context struct {
	filePath        string
	importer        jsonnet.Importer
	extVariables    variablesValues
	nativeFunctions nativeFunctions
	globalBinding   globalBinding
}

type (
	variablesValues map[string]interface{}
	nativeFunctions []*NativeFunction
	globalBinding   map[ast.Identifier]ast.Node
)

type NativeFunction = jsonnet.NativeFunction

// ValueToLiteral converts Go value to jsonnet.Ast literal.
func ValueToLiteral(v interface{}) ast.Node {
	if v == nil {
		return &ast.LiteralNull{}
	}

	switch v := v.(type) {
	case bool:
		return &ast.LiteralBoolean{Value: v}
	case int:
		return &ast.LiteralNumber{OriginalString: cast.ToString(v)}
	case int32:
		return &ast.LiteralNumber{OriginalString: cast.ToString(v)}
	case int64:
		return &ast.LiteralNumber{OriginalString: cast.ToString(v)}
	case float32:
		return &ast.LiteralNumber{OriginalString: cast.ToString(v)}
	case float64:
		return &ast.LiteralNumber{OriginalString: cast.ToString(v)}
	default:
		return &ast.LiteralString{Value: cast.ToString(v), Kind: ast.StringDouble}
	}
}

func NewContext() *Context {
	return &Context{
		extVariables:  make(variablesValues),
		globalBinding: make(globalBinding),
	}
}

// WithFilePath returns clone of the context with the file name set.
func (c *Context) WithFilePath(filePath string) *Context {
	var clone Context
	if c != nil {
		clone = *c
	}
	clone.filePath = filePath
	return &clone
}

// WithImporter returns clone of the context with the importer set.
func (c *Context) WithImporter(importer jsonnet.Importer) *Context {
	var clone Context
	if c != nil {
		clone = *c
	}
	clone.importer = importer
	return &clone
}

func (c *Context) FilePath() string {
	if c == nil {
		return ""
	}
	return c.filePath
}

// ExtVar registers variable to the JsonNet context.
// Variable can be used in the JsonNet code by: std.extVar("<NAME>").
func (c *Context) ExtVar(name string, value interface{}) {
	c.extVariables.add(name, value)
}

// NativeFunction registers native function to the JsonNet context.
// Function can be called in the JsonNet code by: std.native("<NAME>").
func (c *Context) NativeFunction(f *NativeFunction) {
	c.nativeFunctions.add(f)
}

// NativeFunctionWithAlias registers native function to the JsonNet context and creates alias.
// Function can be called in the JsonNet code by: std.native("<FN_NAME>")(...) or by <FN_NAME>(...)
func (c *Context) NativeFunctionWithAlias(f *NativeFunction) {
	c.nativeFunctions.add(f)

	// Register a shortcut: FN_NAME(...)
	// as an alternative to the standard: std.native("FN_NAME")(...)
	c.GlobalBinding(f.Name, &ast.Apply{
		Target: &ast.Index{
			Target: &ast.Var{Id: "std"},
			Index:  &ast.LiteralString{Value: "native"},
		},
		Arguments: ast.Arguments{Positional: []ast.CommaSeparatedExpr{{Expr: &ast.LiteralString{Value: f.Name}}}},
	})
}

func (c *Context) GlobalBinding(identifier string, body ast.Node) {
	c.globalBinding[ast.Identifier(identifier)] = body
}

func (c *Context) registerTo(vm *jsonnet.VM) {
	if c == nil {
		return
	}

	if c.importer != nil {
		vm.Importer(c.importer)
	}

	c.extVariables.registerTo(vm)
	c.nativeFunctions.registerTo(vm)
	c.globalBinding.registerTo(vm)
}

func (v *nativeFunctions) add(f *NativeFunction) {
	*v = append(*v, f)
}

func (v nativeFunctions) registerTo(vm *jsonnet.VM) {
	for _, f := range v {
		vm.NativeFunction(f)
	}
}

func (v globalBinding) registerTo(vm *jsonnet.VM) {
	for identifier, body := range v {
		vm.Bind(identifier, body)
	}
}

func (v variablesValues) add(name string, value interface{}) {
	if _, found := v[name]; found {
		panic(fmt.Errorf(`variable "%s" is already defined`, name))
	}
	v[name] = value
}

func (v variablesValues) registerTo(vm *jsonnet.VM) {
	for k, v := range v {
		vm.ExtNode(k, ValueToLiteral(v))
	}
}
