package jsonnet

import (
	"testing"

	"github.com/google/go-jsonnet"
	"github.com/google/go-jsonnet/ast"
	"github.com/stretchr/testify/assert"
)

func TestVM_Evaluate(t *testing.T) {
	t.Parallel()

	pool := NewPool(
		func(vm *VM) *jsonnet.VM {
			realVM := jsonnet.MakeVM()
			realVM.Importer(NewNopImporter())

			return realVM
		},
	)

	vm := pool.Get()

	out, err := vm.Evaluate("local test = 0; {a: test}", nil)
	assert.NoError(t, err)
	assert.Equal(t, `{"a":0}`, out)

	_, err = vm.Evaluate("{a: test}", nil)
	assert.EqualError(t, err, "1:5-9 Unknown variable: test")
}

func TestVM_Validate_Simple(t *testing.T) {
	t.Parallel()

	pool := NewPool(
		func(vm *VM) *jsonnet.VM {
			realVM := jsonnet.MakeVM()
			realVM.Importer(NewNopImporter())
			return realVM
		},
	)

	vm := pool.Get()

	err := vm.Validate("{a: test()}")
	assert.EqualError(t, err, "1:5-9 Unknown variable: test")

	err = vm.Validate("local test = 0; {a: test()}")
	assert.NoError(t, err)
}

func TestVM_Validate_ShadowedGlobal(t *testing.T) {
	t.Parallel()

	pool := NewPool(
		func(vm *VM) *jsonnet.VM {
			realVM := jsonnet.MakeVM()
			realVM.Importer(NewNopImporter())

			realVM.NativeFunction(&NativeFunction{
				Name:   "test",
				Func:   func(args []any) (any, error) { return nil, nil },
				Params: []ast.Identifier{},
			})
			realVM.Bind("test", Alias("test"))

			return realVM
		},
	)

	vm := pool.Get()

	err := vm.Validate("{a: test()}")
	assert.NoError(t, err)

	err = vm.Validate("local test = 0; {a: test()}")
	assert.NoError(t, err)
}
