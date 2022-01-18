package input

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTemplateInputsValidateDefinitions(t *testing.T) {
	t.Parallel()

	// Fail - Id with a dash
	inputs := Inputs{{
		Id:          "input-id",
		Name:        "input",
		Description: "input desc",
		Type:        "string",
		Default:     "def",
		Kind:        "input",
	}}
	err := inputs.ValidateDefinitions()
	assert.Error(t, err)
	assert.Equal(t, `id can only contain alphanumeric characters, dots and underscores`, err.Error())

	// Fail - type for wrong kind
	inputs = Inputs{{
		Id:          "input.id",
		Name:        "input",
		Description: "input desc",
		Type:        "int",
		Default:     "def",
		Kind:        "password",
	}}
	err = inputs.ValidateDefinitions()
	assert.Error(t, err)
	assert.Equal(t, `- default must be the same type as type or options
- type allowed only for input type`, err.Error())

	// Fail - input Kind with missing Type
	inputs = Inputs{{
		Id:          "input.id",
		Name:        "input",
		Description: "input desc",
		Default:     "def",
		Kind:        "input",
	}}
	err = inputs.ValidateDefinitions()
	assert.Error(t, err)
	assert.Equal(t, `type is a required field`, err.Error())

	// Fail - wrong Rules
	inputs = Inputs{{
		Id:          "input.id",
		Name:        "input",
		Description: "input desc",
		Type:        "int",
		Default:     33,
		Kind:        "input",
		Rules:       "gtex=5",
	}}
	err = inputs.ValidateDefinitions()
	assert.Error(t, err)
	assert.Equal(t, `rules is not valid`, err.Error())

	// Fail - wrong If
	inputs = Inputs{{
		Id:          "input.id2",
		Name:        "input",
		Description: "input desc",
		Type:        "string",
		Kind:        "input",
		If:          "1+(2-1>1",
	}}
	err = inputs.ValidateDefinitions()
	assert.Error(t, err)
	assert.Equal(t, `if is not valid`, err.Error())

	// Success - int Default and empty Options
	inputs = Inputs{{
		Id:          "input.id",
		Name:        "input",
		Description: "input desc",
		Type:        "int",
		Default:     33,
		Options:     Options{},
		Kind:        "input",
		Rules:       "gte=5",
		If:          "1+(2-1)>1",
	}}
	err = inputs.ValidateDefinitions()
	assert.NoError(t, err)

	// Success - no Default
	inputs = Inputs{{
		Id:          "input.id2",
		Name:        "input",
		Description: "input desc",
		Type:        "string",
		Kind:        "input",
	}}
	err = inputs.ValidateDefinitions()
	assert.NoError(t, err)
}

func TestTemplateInputsValidateDefinitionsSelect(t *testing.T) {
	t.Parallel()

	// Fail - defined options for wrong Kind
	inputs := Inputs{{
		Id:          "input.id",
		Name:        "input",
		Description: "input desc",
		Default:     "def",
		Options: Options{
			{Id: "a", Name: "A"},
			{Id: "b", Name: "B"},
		},
		Kind: "input",
	}}
	err := inputs.ValidateDefinitions()
	assert.Error(t, err)
	assert.Equal(t, `- type is a required field
- options allowed only for select and multiselect`, err.Error())

	// Fail - empty Options
	inputs = Inputs{{
		Id:          "input.id",
		Name:        "input",
		Description: "input desc",
		Options:     Options{},
		Kind:        "select",
	}}
	err = inputs.ValidateDefinitions()
	assert.Error(t, err)
	assert.Equal(t, `options allowed only for select and multiselect`, err.Error())

	// Fail - Default value missing in Options
	inputs = Inputs{{
		Id:          "input.id",
		Name:        "input",
		Description: "input desc",
		Default:     "c",
		Options: Options{
			{Id: "a", Name: "A"},
			{Id: "b", Name: "B"},
		},
		Kind: "select",
	}}
	err = inputs.ValidateDefinitions()
	assert.Error(t, err)
	assert.Equal(t, `default must be the same type as type or options`, err.Error())

	// Success - with Options
	inputs = Inputs{{
		Id:          "input.id",
		Name:        "input",
		Description: "input desc",
		Default:     "a",
		Options: Options{
			{Id: "a", Name: "A"},
			{Id: "b", Name: "B"},
		},
		Kind: "select",
	}}
	err = inputs.ValidateDefinitions()
	assert.NoError(t, err)

	// Fail - Default value missing in MultiOptions
	inputs = Inputs{{
		Id:          "input.id",
		Name:        "input",
		Description: "input desc",
		Default:     []string{"a", "d"},
		Options: Options{
			{Id: "a", Name: "A"},
			{Id: "b", Name: "B"},
			{Id: "c", Name: "C"},
		},
		Kind: "multiselect",
	}}
	err = inputs.ValidateDefinitions()
	assert.Error(t, err)
	assert.Equal(t, `default must be the same type as type or options`, err.Error())

	// Success - Default for MultiOptions
	inputs = Inputs{{
		Id:          "input.id",
		Name:        "input",
		Description: "input desc",
		Default:     []string{"a", "c"},
		Options: Options{
			{Id: "a", Name: "A"},
			{Id: "b", Name: "B"},
			{Id: "c", Name: "C"},
		},
		Kind: "multiselect",
	}}
	err = inputs.ValidateDefinitions()
	assert.NoError(t, err)
}

const inputsJson = `[
  {
    "id": "fb.extractor.username",
    "name": "Facebook username",
    "description": "Facebook username description",
    "kind": "input",
    "type": "string"
  },
  {
    "id": "fb.extractor.password",
    "name": "Facebook password",
    "description": "Facebook password description",
    "kind": "password"
  },
  {
    "id": "fb.extractor.options",
    "name": "Facebook options",
    "description": "Facebook options description",
    "kind": "select",
    "options": [
      {
        "id": "a",
        "name": "A"
      },
      {
        "id": "b",
        "name": "B"
      }
    ]
  }
]`

func TestTemplateInputsJsonUnmarshal(t *testing.T) {
	t.Parallel()

	var inputs Inputs
	assert.NoError(t, json.Unmarshal([]byte(inputsJson), &inputs))
	assert.Equal(t, Inputs{
		{
			Id:          "fb.extractor.username",
			Name:        "Facebook username",
			Description: "Facebook username description",
			Type:        "string",
			Kind:        "input",
		},
		{
			Id:          "fb.extractor.password",
			Name:        "Facebook password",
			Description: "Facebook password description",
			Kind:        "password",
		},
		{
			Id:          "fb.extractor.options",
			Name:        "Facebook options",
			Description: "Facebook options description",
			Kind:        "select",
			Options: Options{
				{Id: "a", Name: "A"},
				{Id: "b", Name: "B"},
			},
		},
	}, inputs)
}

func TestTemplateInputsJsonMarshal(t *testing.T) {
	t.Parallel()

	inputs := Inputs{
		&Input{
			Id:          "fb.extractor.username",
			Name:        "Facebook username",
			Description: "Facebook username description",
			Type:        "string",
			Kind:        "input",
		},
		&Input{
			Id:          "fb.extractor.password",
			Name:        "Facebook password",
			Description: "Facebook password description",
			Kind:        "password",
		},
		&Input{
			Id:          "fb.extractor.options",
			Name:        "Facebook options",
			Description: "Facebook options description",
			Kind:        "select",
			Options: Options{
				{Id: "a", Name: "A"},
				{Id: "b", Name: "B"},
			},
		},
	}
	resultJson, err := json.MarshalIndent(inputs, "", "  ")
	assert.NoError(t, err)
	assert.Equal(t, inputsJson, string(resultJson))
}

func TestTemplateInputValidateUserInput(t *testing.T) {
	t.Parallel()

	input := &Input{
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

	input = &Input{
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

func TestTemplateInputAvailable(t *testing.T) {
	t.Parallel()

	// Check If evaluated as true
	input := &Input{
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
	input = &Input{
		Id:          "input.id",
		Name:        "input",
		Description: "input description",
		Kind:        "input",
		Type:        "int",
	}
	assert.True(t, input.Available(nil))

	// Check If evaluated as false
	input = &Input{
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
