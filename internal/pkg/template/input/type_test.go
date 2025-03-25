package input

import (
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestType_IsValid(t *testing.T) {
	t.Parallel()
	assert.True(t, slices.Contains(allTypes(), TypeString))
	assert.True(t, slices.Contains(allTypes(), TypeInt))
	assert.True(t, slices.Contains(allTypes(), TypeDouble))
	assert.True(t, slices.Contains(allTypes(), TypeBool))
	assert.True(t, slices.Contains(allTypes(), TypeStringArray))
	assert.False(t, slices.Contains(allTypes(), Type("foo")))
}

func TestType_ValidateValue(t *testing.T) {
	t.Parallel()

	// String
	require.NoError(t, TypeString.ValidateValue(reflect.ValueOf("")))
	require.NoError(t, TypeString.ValidateValue(reflect.ValueOf("foo")))
	err := TypeString.ValidateValue(reflect.ValueOf(nil))
	require.Error(t, err)
	assert.Equal(t, "should be string, got null", err.Error())
	err = TypeString.ValidateValue(reflect.ValueOf(123))
	require.Error(t, err)
	assert.Equal(t, "should be string, got int", err.Error())

	// Int
	require.NoError(t, TypeInt.ValidateValue(reflect.ValueOf(0)))
	require.NoError(t, TypeInt.ValidateValue(reflect.ValueOf(-123)))
	err = TypeInt.ValidateValue(reflect.ValueOf(nil))
	require.Error(t, err)
	assert.Equal(t, "should be int, got null", err.Error())
	err = TypeInt.ValidateValue(reflect.ValueOf("foo"))
	require.Error(t, err)
	assert.Equal(t, "should be int, got string", err.Error())

	// Double
	require.NoError(t, TypeDouble.ValidateValue(reflect.ValueOf(0.0)))
	require.NoError(t, TypeDouble.ValidateValue(reflect.ValueOf(-1.23)))
	err = TypeDouble.ValidateValue(reflect.ValueOf(nil))
	require.Error(t, err)
	assert.Equal(t, "should be double, got null", err.Error())
	err = TypeDouble.ValidateValue(reflect.ValueOf("foo"))
	require.Error(t, err)
	assert.Equal(t, "should be double, got string", err.Error())

	// Bool
	require.NoError(t, TypeBool.ValidateValue(reflect.ValueOf(true)))
	require.NoError(t, TypeBool.ValidateValue(reflect.ValueOf(false)))
	err = TypeBool.ValidateValue(reflect.ValueOf(nil))
	require.Error(t, err)
	assert.Equal(t, "should be bool, got null", err.Error())
	err = TypeBool.ValidateValue(reflect.ValueOf("foo"))
	require.Error(t, err)
	assert.Equal(t, "should be bool, got string", err.Error())

	// String[]
	require.NoError(t, TypeStringArray.ValidateValue(reflect.ValueOf([]string{"foo", "bar"})))
	require.NoError(t, TypeStringArray.ValidateValue(reflect.ValueOf([]any{"foo", "bar"})))
	err = TypeStringArray.ValidateValue(reflect.ValueOf(nil))
	require.Error(t, err)
	assert.Equal(t, "should be array, got null", err.Error())
	err = TypeStringArray.ValidateValue(reflect.ValueOf("foo"))
	require.Error(t, err)
	assert.Equal(t, "should be array, got string", err.Error())
	err = TypeStringArray.ValidateValue(reflect.ValueOf([]any{"foo", 123}))
	require.Error(t, err)
	assert.Equal(t, "all items should be string, got int, index 1", err.Error())

	// Object
	require.NoError(t, TypeObject.ValidateValue(reflect.ValueOf(map[string]any{"a": "b"})))
	err = TypeObject.ValidateValue(reflect.ValueOf(nil))
	require.Error(t, err)
	assert.Equal(t, "should be object, got null", err.Error())
	err = TypeObject.ValidateValue(reflect.ValueOf("foo"))
	require.Error(t, err)
	assert.Equal(t, "should be object, got string", err.Error())
}

func TestType_ParseValue(t *testing.T) {
	t.Parallel()
	cases := []struct {
		t      Type
		input  any
		output any
		err    string
	}{
		{TypeInt, "", 0, ""},
		{TypeInt, 123, 123, ""},
		{TypeInt, 123.0, 123, ""},
		{TypeInt, 123.45, nil, `value "123.45" is not integer`},
		{TypeInt, "123", 123, ""},
		{TypeInt, "123.45", nil, `value "123.45" is not integer`},
		{TypeDouble, "", 0.0, ""},
		{TypeDouble, 123, 123.0, ""},
		{TypeDouble, 123.0, 123.0, ""},
		{TypeDouble, 123.45, 123.45, ""},
		{TypeDouble, "123", 123.0, ""},
		{TypeDouble, "123.45", 123.45, ""},
		{TypeBool, "", false, ``},
		{TypeBool, 123, nil, `value "123" is not bool`},
		{TypeBool, 123.45, nil, `value "123.45" is not bool`},
		{TypeBool, "123", nil, `value "123" is not bool`},
		{TypeBool, "true", true, ""},
		{TypeBool, "false", false, ""},
		{TypeBool, true, true, ""},
		{TypeBool, false, false, ""},
		{TypeString, "", "", ""},
		{TypeString, 123, "123", ""},
		{TypeString, 123.45, "123.45", ""},
		{TypeString, true, "true", ""},
		{TypeString, "abc", "abc", ""},
		{TypeStringArray, "", []any{}, ""},
		{TypeStringArray, "a,b", []any{"a", "b"}, ""},
		{TypeStringArray, []string{}, []any{}, ""},
		{TypeStringArray, []string{"a", "b"}, []any{"a", "b"}, ""},
		{TypeStringArray, []any{}, []any{}, ""},
		{TypeStringArray, []any{"a", "b"}, []any{"a", "b"}, ""},
		{TypeStringArray, 123, nil, "unexpected type \"int\""},
	}

	// Assert
	for i, c := range cases {
		desc := fmt.Sprintf("case %d", i)
		actual, err := c.t.ParseValue(c.input)
		assert.Equal(t, c.output, actual, desc)
		if c.err == "" {
			require.NoError(t, err, desc)
		} else {
			require.Error(t, err, desc)
			assert.Equal(t, c.err, err.Error(), desc)
		}
	}
}

func TestType_EmptyValue(t *testing.T) {
	t.Parallel()
	cases := []struct {
		t      Type
		output any
	}{
		{TypeInt, 0},
		{TypeDouble, 0.0},
		{TypeBool, false},
		{TypeString, ""},
		{TypeStringArray, []any{}},
		{TypeObject, make(map[string]any)},
	}

	// Assert
	for i, c := range cases {
		desc := fmt.Sprintf("case %d", i)
		actual := c.t.EmptyValue()
		assert.Equal(t, c.output, actual, desc)
	}
}
