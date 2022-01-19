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
		Kind:        "input",
		Type:        "int",
		Rules:       "gte=5,lte=10",
	}
	err := input.ValidateUserInput(1, nil)
	assert.Error(t, err)
	assert.Equal(t, "input must be 5 or greater", err.Error())

	err = input.ValidateUserInput("1", nil)
	assert.Error(t, err)
	assert.Equal(t, "input should have type int, got string instead", err.Error())

	assert.Error(t, err)
	assert.NoError(t, input.ValidateUserInput(7, nil))

	input = Input{
		Id:          "input.id",
		Name:        "input",
		Description: "input description",
		Kind:        "confirm",
	}
	err = input.ValidateUserInput(1, nil)
	assert.Error(t, err)
	assert.Equal(t, "input should be a bool, got int", err.Error())
	assert.NoError(t, input.ValidateUserInput(true, nil))
}

func TestInput_Available(t *testing.T) {
	t.Parallel()

	// Check If evaluated as true
	input := Input{
		Id:          "input.id",
		Name:        "input",
		Description: "input description",
		Kind:        "input",
		Type:        "int",
		If:          "facebook_integration == true",
	}
	params := make(map[string]interface{}, 1)
	params["facebook_integration"] = true
	assert.True(t, input.Available(params))

	// Check empty If evaluated as true
	input = Input{
		Id:          "input.id",
		Name:        "input",
		Description: "input description",
		Kind:        "input",
		Type:        "int",
	}
	assert.True(t, input.Available(nil))

	// Check If evaluated as false
	input = Input{
		Id:          "input.id",
		Name:        "input",
		Description: "input description",
		Kind:        "input",
		Type:        "int",
		If:          "facebook_integration == true",
	}
	params = make(map[string]interface{}, 1)
	params["facebook_integration"] = false
	assert.False(t, input.Available(params))
}
