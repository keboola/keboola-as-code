package jsonnet

import (
	"fmt"
	"sort"

	"github.com/google/go-jsonnet"
	"github.com/google/go-jsonnet/ast"
	"github.com/spf13/cast"
)

type Context struct {
	extVariables    variablesValues
	nativeFunctions nativeFunctions
	localAliases    localAliases
}

type (
	variablesValues map[string]interface{}
	nativeFunctions []*jsonnet.NativeFunction
	localAliases    map[string]ast.Node
)

func NewContext() *Context {
	return &Context{
		extVariables: make(variablesValues),
		localAliases: make(localAliases),
	}
}

// ExtVar registers variable to the JsonNet context.
// Variable can be used in the JsonNet code by: std.extVar("<NAME>").
func (c *Context) ExtVar(name string, value interface{}) {
	c.extVariables.add(name, value)
}

// NativeFunction registers native function to the JsonNet context.
// Function can be called in the JsonNet code by: std.native("<NAME>").
func (c *Context) NativeFunction(f *jsonnet.NativeFunction) {
	c.nativeFunctions.add(f)
}

// NativeFunctionWithAlias registers native function to the JsonNet context and creates alias.
// Function can be called in the JsonNet code by: std.native("<NAME>")(...) or by <NAME>(...)
func (c *Context) NativeFunctionWithAlias(f *jsonnet.NativeFunction) {
	c.nativeFunctions.add(f)
	code := fmt.Sprintf("std.native(\"%s\")", f.Name)
	if err := c.localAliases.add(f.Name, code); err != nil {
		panic(err)
	}
}

// LocalAlias registers alias to the JsonNet context.
// Alias is added to the beginning of the code as: local <NAME> = <CODE>
// Alias can be used in the JsonNet code by: <NAME>.
func (c *Context) LocalAlias(name, code string) {
	if err := c.localAliases.add(name, code); err != nil {
		panic(err)
	}
}

func (c *Context) wrapAst(input ast.Node) ast.Node {
	if c == nil {
		return input
	}
	return c.localAliases.wrapAst(input)
}

func (c *Context) registerTo(vm *jsonnet.VM) {
	if c == nil {
		return
	}
	c.extVariables.registerTo(vm)
	c.nativeFunctions.registerTo(vm)
}

func (v localAliases) wrapAst(input ast.Node) ast.Node {
	return &ast.Local{
		Binds: v.binds(),
		Body:  input,
	}
}

func (v localAliases) add(name string, code string) error {
	if _, found := v[name]; found {
		panic(fmt.Errorf(`alias "%s" is already defined`, name))
	}

	node, err := ToAst(code)
	if err != nil {
		return err
	}
	v[name] = node
	return nil
}

func (v localAliases) binds() ast.LocalBinds {
	output := make(ast.LocalBinds, 0)

	for name, node := range v {
		binding := ast.LocalBind{
			Variable: ast.Identifier(name),
			Body:     node,
		}
		output = append(output, binding)
	}

	// Sort by name
	sort.SliceStable(output, func(i, j int) bool {
		return output[i].Variable < output[j].Variable
	})

	return output
}

func (v *nativeFunctions) add(f *jsonnet.NativeFunction) {
	*v = append(*v, f)
}

func (v nativeFunctions) registerTo(vm *jsonnet.VM) {
	for _, f := range v {
		vm.NativeFunction(f)
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
