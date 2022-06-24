package input

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInput_ValidateUserInput(t *testing.T) {
	t.Parallel()

	input := Input{
		Id:          "input.id",
		Name:        "input",
		Description: "input description",
		Type:        "int",
		Kind:        "input",
		Rules:       "gte=5,lte=10",
	}
	err := input.ValidateUserInput(1)
	assert.Error(t, err)
	assert.Equal(t, "input.id must be 5 or greater", err.Error())

	err = input.ValidateUserInput("1")
	assert.Error(t, err)
	assert.Equal(t, "input should be int, got string", err.Error())

	assert.Error(t, err)
	assert.NoError(t, input.ValidateUserInput(7))

	input = Input{
		Id:          "input.id",
		Name:        "input",
		Description: "input description",
		Type:        "bool",
		Kind:        "confirm",
	}
	err = input.ValidateUserInput(1)
	assert.Error(t, err)
	assert.Equal(t, "input should be bool, got int", err.Error())
	assert.NoError(t, input.ValidateUserInput(true))
}

func TestInput_ValidateUserInputOAuth(t *testing.T) {
	t.Parallel()

	input := Input{
		Id:          "input.oauth",
		Name:        "oauth",
		Description: "oauth",
		Type:        "object",
		Kind:        "oauth",
	}
	err := input.ValidateUserInput([]string{"one", "two"})
	assert.Error(t, err)
	assert.Equal(t, "oauth should be object, got slice", err.Error())

	err = input.ValidateUserInput(map[string]interface{}{})
	assert.Error(t, err)
	assert.Equal(t, "oauth must not be empty", err.Error())

	err = input.ValidateUserInput(map[string]interface{}{"a": "b"})
	assert.NoError(t, err)
}

func TestInput_Available(t *testing.T) {
	t.Parallel()

	// Check If evaluated as true
	input := Input{
		Id:          "input.id",
		Name:        "input",
		Description: "input description",
		Type:        "int",
		Kind:        "input",
		If:          "facebook_integration == true",
	}
	params := make(map[string]interface{}, 1)
	params["facebook_integration"] = true
	v, err := input.Available(params)
	assert.True(t, v)
	assert.NoError(t, err)

	// Check empty If evaluated as true
	input = Input{
		Id:          "input.id",
		Name:        "input",
		Description: "input description",
		Type:        "int",
		Kind:        "input",
	}
	v, err = input.Available(nil)
	assert.True(t, v)
	assert.NoError(t, err)

	// Check If evaluated as false
	input = Input{
		Id:          "input.id",
		Name:        "input",
		Description: "input description",
		Type:        "int",
		Kind:        "input",
		If:          "facebook_integration == true",
	}
	params = make(map[string]interface{}, 1)
	params["facebook_integration"] = false
	v, err = input.Available(params)
	assert.False(t, v)
	assert.NoError(t, err)
}
