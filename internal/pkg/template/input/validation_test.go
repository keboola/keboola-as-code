package input

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateUserInputTypeByKind(t *testing.T) {
	t.Parallel()

	// Confirm Kind
	err := validateUserInputTypeByKind("string", "confirm", "input")
	assert.Error(t, err)
	assert.Equal(t, "input should be a bool, got string", err.Error())
	assert.NoError(t, validateUserInputTypeByKind(true, "confirm", "input"))

	// Password Kind
	assert.Error(t, validateUserInputTypeByKind(123, "password", "input"))
	assert.NoError(t, validateUserInputTypeByKind("string", "password", "input"))

	// Textarea Kind
	assert.Error(t, validateUserInputTypeByKind(false, "textarea", "input"))
	assert.NoError(t, validateUserInputTypeByKind("string", "textarea", "input"))
}

func TestValidateUserInputByType(t *testing.T) {
	t.Parallel()

	err := validateUserInputByType("str", "string", "input")
	assert.NoError(t, err)

	err = validateUserInputByType(3, "string", "input")
	assert.Equal(t, "input should have type string, got int instead", err.Error())
	assert.Error(t, err)

	err = validateUserInputByType(3, "int", "input")
	assert.NoError(t, err)

	err = validateUserInputByType("3", "int", "input")
	assert.Error(t, err)
}
