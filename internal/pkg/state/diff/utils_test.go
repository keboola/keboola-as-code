package diff

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

func TestOnlyOnceTransformer(t *testing.T) {
	// Both values are transformed to the "foo"
	transformFn := func(str string) interface{} {
		return "foo"
	}

	// Without OnlyOnceTransformer
	assert.Panics(t, func() {
		cmp.Equal("a", "b", cmp.Transformer("test", transformFn))
	})

	// With OnlyOnceTransformer
	assert.True(t, cmp.Equal("a", "b", OnlyOnceTransformer("test", transformFn)))
}

func TestCoreType(t *testing.T) {
	// Nop
	v1 := "value"
	vx, tx := CoreType(reflect.ValueOf(v1))
	assert.IsType(t, "string", vx.Interface())
	assert.Equal(t, reflect.TypeOf(v1), tx)

	// Pointer
	v2 := &v1
	vx, tx = CoreType(reflect.ValueOf(v2))
	assert.IsType(t, "string", vx.Interface())
	assert.Equal(t, reflect.TypeOf(v1), tx)

	// Interface
	v3 := interface{}(v2)
	vx, tx = CoreType(reflect.ValueOf(v3))
	assert.IsType(t, "string", vx.Interface())
	assert.Equal(t, reflect.TypeOf(v1), tx)

	// Pointer to interface
	v4 := &v3
	vx, tx = CoreType(reflect.ValueOf(v4))
	assert.IsType(t, "string", vx.Interface())
	assert.Equal(t, reflect.TypeOf(v1), tx)
}
