package jsonnet

import (
	"testing"

	"github.com/google/go-jsonnet/ast"
	"github.com/stretchr/testify/assert"
)

func TestValidate_Simple(t *testing.T) {
	t.Parallel()

	ctx := NewContext().WithFilePath("code")

	err := ctx.Validate("{a: test()}")
	assert.EqualError(t, err, "code:1:5-9 Unknown variable: test")

	ctx.NativeFunctionWithAlias(&NativeFunction{
		Name:   "test",
		Func:   func(args []any) (any, error) { return nil, nil },
		Params: []ast.Identifier{},
	})

	err = ctx.Validate("{a: test()}")
	assert.NoError(t, err)
}

func TestValidate_ShadowedGlobal(t *testing.T) {
	t.Parallel()

	ctx := NewContext().WithFilePath("code")

	err := ctx.Validate("local test = 0; {a: test()}")
	assert.NoError(t, err)

	ctx.NativeFunctionWithAlias(&NativeFunction{
		Name:   "test",
		Func:   func(args []any) (any, error) { return nil, nil },
		Params: []ast.Identifier{},
	})

	err = ctx.Validate("local test = 0; {a: test()}")
	assert.NoError(t, err)
}
