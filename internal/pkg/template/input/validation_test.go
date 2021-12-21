package input

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateUserInputTypeByKind(t *testing.T) {
	t.Parallel()

	// Confirm Kind
	assert.Error(t, validateUserInputTypeByKind("string", "confirm"))
	assert.NoError(t, validateUserInputTypeByKind(true, "confirm"))

	// Password Kind
	assert.Error(t, validateUserInputTypeByKind(123, "password"))
	assert.NoError(t, validateUserInputTypeByKind("string", "password"))

	// Textarea Kind
	assert.Error(t, validateUserInputTypeByKind(false, "textarea"))
	assert.NoError(t, validateUserInputTypeByKind("string", "textarea"))
}

func TestValidateUserInputByType(t *testing.T) {
	t.Parallel()

	err := validateUserInputByType("str", "string")
	assert.NoError(t, err)

	err = validateUserInputByType(3, "string")
	assert.Error(t, err)

	err = validateUserInputByType(3, "int")
	assert.NoError(t, err)

	err = validateUserInputByType("3", "int")
	assert.Error(t, err)
}
