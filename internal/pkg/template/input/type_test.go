package input

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestType_IsValid(t *testing.T) {
	t.Parallel()
	assert.True(t, TypeString.IsValid())
	assert.True(t, TypeInt.IsValid())
	assert.True(t, TypeDouble.IsValid())
	assert.True(t, TypeBool.IsValid())
	assert.True(t, TypeStringArray.IsValid())
	assert.False(t, Type("foo").IsValid())
}

func TestType_ValidateValue(t *testing.T) {
	t.Parallel()

	// String
	assert.NoError(t, TypeString.ValidateValue(reflect.ValueOf("")))
	assert.NoError(t, TypeString.ValidateValue(reflect.ValueOf("foo")))
	err := TypeString.ValidateValue(reflect.ValueOf(nil))
	assert.Error(t, err)
	assert.Equal(t, "should be string, got null", err.Error())
	err = TypeString.ValidateValue(reflect.ValueOf(123))
	assert.Error(t, err)
	assert.Equal(t, "should be string, got int", err.Error())

	// Int
	assert.NoError(t, TypeInt.ValidateValue(reflect.ValueOf(0)))
	assert.NoError(t, TypeInt.ValidateValue(reflect.ValueOf(-123)))
	err = TypeInt.ValidateValue(reflect.ValueOf(nil))
	assert.Error(t, err)
	assert.Equal(t, "should be int, got null", err.Error())
	err = TypeInt.ValidateValue(reflect.ValueOf("foo"))
	assert.Error(t, err)
	assert.Equal(t, "should be int, got string", err.Error())

	// Double
	assert.NoError(t, TypeDouble.ValidateValue(reflect.ValueOf(0.0)))
	assert.NoError(t, TypeDouble.ValidateValue(reflect.ValueOf(-1.23)))
	err = TypeDouble.ValidateValue(reflect.ValueOf(nil))
	assert.Error(t, err)
	assert.Equal(t, "should be double, got null", err.Error())
	err = TypeDouble.ValidateValue(reflect.ValueOf("foo"))
	assert.Error(t, err)
	assert.Equal(t, "should be double, got string", err.Error())

	// Bool
	assert.NoError(t, TypeBool.ValidateValue(reflect.ValueOf(true)))
	assert.NoError(t, TypeBool.ValidateValue(reflect.ValueOf(false)))
	err = TypeBool.ValidateValue(reflect.ValueOf(nil))
	assert.Error(t, err)
	assert.Equal(t, "should be bool, got null", err.Error())
	err = TypeBool.ValidateValue(reflect.ValueOf("foo"))
	assert.Error(t, err)
	assert.Equal(t, "should be bool, got string", err.Error())

	// String[]
	assert.NoError(t, TypeStringArray.ValidateValue(reflect.ValueOf([]string{"foo", "bar"})))
	assert.NoError(t, TypeStringArray.ValidateValue(reflect.ValueOf([]interface{}{"foo", "bar"})))
	err = TypeStringArray.ValidateValue(reflect.ValueOf(nil))
	assert.Error(t, err)
	assert.Equal(t, "should be array, got null", err.Error())
	err = TypeStringArray.ValidateValue(reflect.ValueOf("foo"))
	assert.Error(t, err)
	assert.Equal(t, "should be array, got string", err.Error())
	err = TypeStringArray.ValidateValue(reflect.ValueOf([]interface{}{"foo", 123}))
	assert.Error(t, err)
	assert.Equal(t, "all items should be string, got int, index 1", err.Error())
}
