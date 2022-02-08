package input

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKind_IsValid(t *testing.T) {
	t.Parallel()
	assert.True(t, KindInput.IsValid())
	assert.True(t, KindPassword.IsValid())
	assert.True(t, KindTextarea.IsValid())
	assert.True(t, KindConfirm.IsValid())
	assert.True(t, KindSelect.IsValid())
	assert.True(t, KindMultiSelect.IsValid())
	assert.False(t, Kind("foo").IsValid())
}

func TestKind_ValidateType(t *testing.T) {
	t.Parallel()
	// Input
	assert.NoError(t, KindInput.ValidateType(TypeString))
	assert.NoError(t, KindInput.ValidateType(TypeInt))
	assert.NoError(t, KindInput.ValidateType(TypeDouble))
	err := KindInput.ValidateType(TypeBool)
	assert.Error(t, err)
	assert.Equal(t, "should be one of [string, int, double] for kind=input, found bool", err.Error())
	err = KindInput.ValidateType(TypeStringArray)
	assert.Error(t, err)
	assert.Equal(t, "should be one of [string, int, double] for kind=input, found string[]", err.Error())

	// Password
	assert.NoError(t, KindPassword.ValidateType(TypeString))
	err = KindPassword.ValidateType(TypeInt)
	assert.Error(t, err)
	assert.Equal(t, "should be string for kind=password, found int", err.Error())

	// Confirm
	assert.NoError(t, KindConfirm.ValidateType(TypeBool))
	err = KindConfirm.ValidateType(TypeInt)
	assert.Error(t, err)
	assert.Equal(t, "should be bool for kind=confirm, found int", err.Error())

	// Select
	assert.NoError(t, KindSelect.ValidateType(TypeString))
	err = KindSelect.ValidateType(TypeStringArray)
	assert.Error(t, err)
	assert.Equal(t, "should be string for kind=select, found string[]", err.Error())

	// MultiSelect
	assert.NoError(t, KindMultiSelect.ValidateType(TypeStringArray))
	err = KindMultiSelect.ValidateType(TypeString)
	assert.Error(t, err)
	assert.Equal(t, "should be string[] for kind=multiselect, found string", err.Error())
}
