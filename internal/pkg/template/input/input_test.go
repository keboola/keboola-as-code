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
	assert.Contains(t, err.Error(), `key="id"`)

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
	assert.Contains(t, err.Error(), `key="type"`)

	// Fail - defined options for wrong Kind
	inputs = Inputs{{
		Id:          "input.id",
		Name:        "input",
		Description: "input desc",
		Default:     "def",
		Options:     []Option{"a", "b"},
		Kind:        "input",
	}}
	err = inputs.ValidateDefinitions()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `key="options"`)

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
	assert.Contains(t, err.Error(), `key="type"`)
	assert.Contains(t, err.Error(), `failed "required_if"`)

	// Fail - Default value missing in Options
	inputs = Inputs{{
		Id:          "input.id",
		Name:        "input",
		Description: "input desc",
		Default:     "c",
		Options:     []Option{"a", "b"},
		Kind:        "input",
	}}
	err = inputs.ValidateDefinitions()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `failed "template-input-options"`)

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
	assert.Contains(t, err.Error(), `failed "template-input-rules"`)

	// Success - with Options
	inputs = Inputs{{
		Id:          "input.id",
		Name:        "input",
		Description: "input desc",
		Default:     "a",
		Options:     []Option{"a", "b"},
		Kind:        "select",
	}}
	err = inputs.ValidateDefinitions()
	assert.NoError(t, err)

	// Success - int Default and empty Options
	inputs = Inputs{{
		Id:          "input.id",
		Name:        "input",
		Description: "input desc",
		Type:        "int",
		Default:     33,
		Options:     []Option{},
		Kind:        "input",
		Rules:       "gte=5",
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
      "1",
      "2"
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
			Options:     []Option{"1", "2"},
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
			Options:     []Option{"1", "2"},
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
	assert.Error(t, input.ValidateUserInput(1, nil))
	assert.Error(t, input.ValidateUserInput("1", nil))
	assert.NoError(t, input.ValidateUserInput(7, nil))

	input = &Input{
		Id:          "input.id",
		Name:        "input",
		Description: "input description",
		Kind:        "confirm",
	}
	assert.Error(t, input.ValidateUserInput(1, nil))
	assert.NoError(t, input.ValidateUserInput(true, nil))
}
