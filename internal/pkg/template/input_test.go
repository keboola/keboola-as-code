package template

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestTemplateInput(t *testing.T) {
	t.Parallel()

	// Fail - Id with a number
	input := &Input{
		Id:          "fb.extractor.password2",
		Name:        "input",
		Description: "input desc",
		Type:        "string",
		Default:     "def",
		Kind:        "input",
	}
	err := validator.Validate(input)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `key="id"`)

	// Fail - Id with a dash
	input = &Input{
		Id:          "fb.extractor-password",
		Name:        "input",
		Description: "input desc",
		Type:        "string",
		Default:     "def",
		Kind:        "input",
	}
	err = validator.Validate(input)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `key="id"`)

	// Fail - wrong default type
	input = &Input{
		Id:          "fb.extractor.password",
		Name:        "input",
		Description: "input desc",
		Type:        "int",
		Default:     "def",
		Kind:        "input",
	}
	err = validator.Validate(input)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `key="default"`)

	// Fail - defined options for wrong Kind
	input = &Input{
		Id:          "fb.extractor.password",
		Name:        "input",
		Description: "input desc",
		Type:        "string",
		Default:     "def",
		Options:     []Option{"a", "b"},
		Kind:        "input",
	}
	err = validator.Validate(input)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `key="options"`)

	// Fail - defined options with wrong Type
	input = &Input{
		Id:          "fb.extractor.password",
		Name:        "input",
		Description: "input desc",
		Type:        "string",
		Default:     "def",
		Options:     []Option{"a", 1},
		Kind:        "input",
	}
	err = validator.Validate(input)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `key="options"`)

	// Success - with Options
	input = &Input{
		Id:          "fb.extractor.password",
		Name:        "input",
		Description: "input desc",
		Type:        "string",
		Default:     "def",
		Options:     []Option{"a", "b"},
		Kind:        "select",
	}
	err = validator.Validate(input)
	assert.NoError(t, err)

	// Success - int Default and empty Options
	input = &Input{
		Id:          "fb.extractor.password",
		Name:        "input",
		Description: "input desc",
		Type:        "int",
		Default:     33,
		Options:     []Option{},
		Kind:        "input",
	}
	err = validator.Validate(input)
	assert.NoError(t, err)

	// Success - no Default
	input = &Input{
		Id:          "fb.extractor.password",
		Name:        "input",
		Description: "input desc",
		Type:        "string",
		Kind:        "input",
	}
	err = validator.Validate(input)
	assert.NoError(t, err)
}

func TestTemplateInputsJsonUnmarshal(t *testing.T) {
	t.Parallel()

	inputsJson := `
[
  {
    "id": "fb.extractor.username",
    "name": "Facebook username",
    "description": "Facebook username description",
    "type": "string",
    "kind": "input"
  },
  {
    "id": "fb.extractor.password",
    "name": "Facebook password",
    "description": "Facebook password description",
    "type": "string",
    "options": [],
    "kind": "password"
  },
  {
    "id": "fb.extractor.option",
    "name": "Facebook option",
    "description": "Facebook option description",
    "type": "int",
    "options": [1, 2],
    "kind": "select"
  }
]`
	var inputs Inputs
	assert.NoError(t, json.Unmarshal([]byte(inputsJson), &inputs))
	assert.Len(t, inputs, 3)

	assert.IsType(t, &Input{}, inputs[0])
	assert.Equal(t, inputs[0].Id, "fb.extractor.username")
	assert.Equal(t, inputs[0].Name, "Facebook username")
	assert.Equal(t, inputs[0].Description, "Facebook username description")
	assert.Equal(t, inputs[0].Type, "string")
	assert.Equal(t, inputs[0].Kind, "input")
	assert.Len(t, inputs[1].Options, 0)

	assert.IsType(t, &Input{}, inputs[1])
	assert.Equal(t, inputs[1].Id, "fb.extractor.password")
	assert.Equal(t, inputs[1].Name, "Facebook password")
	assert.Equal(t, inputs[1].Description, "Facebook password description")
	assert.Equal(t, inputs[1].Type, "string")
	assert.Equal(t, inputs[1].Kind, "password")
	assert.IsType(t, inputs[1].Options, []Option{})
	assert.Len(t, inputs[1].Options, 0)

	assert.IsType(t, &Input{}, inputs[2])
	assert.Equal(t, inputs[2].Id, "fb.extractor.option")
	assert.Equal(t, inputs[2].Name, "Facebook option")
	assert.Equal(t, inputs[2].Description, "Facebook option description")
	assert.Equal(t, inputs[2].Type, "int")
	assert.Equal(t, inputs[2].Kind, "select")
	assert.IsType(t, inputs[2].Options, []Option{1, 2})
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
			Options:     nil,
		},
		&Input{
			Id:          "fb.extractor.password",
			Name:        "Facebook password",
			Description: "Facebook password description",
			Type:        "string",
			Kind:        "password",
			Options:     nil,
		},
		&Input{
			Id:          "fb.extractor.options",
			Name:        "Facebook options",
			Description: "Facebook options description",
			Type:        "int",
			Kind:        "select",
			Options:     []Option{1, 2},
		},
	}
	inputsJson, err := json.MarshalIndent(inputs, "", "  ")
	assert.NoError(t, err)
	expectedJson := `[
  {
    "id": "fb.extractor.username",
    "name": "Facebook username",
    "description": "Facebook username description",
    "type": "string",
    "kind": "input"
  },
  {
    "id": "fb.extractor.password",
    "name": "Facebook password",
    "description": "Facebook password description",
    "type": "string",
    "kind": "password"
  },
  {
    "id": "fb.extractor.options",
    "name": "Facebook options",
    "description": "Facebook options description",
    "type": "int",
    "kind": "select",
    "options": [
      1,
      2
    ]
  }
]`
	assert.Equal(t, expectedJson, string(inputsJson))
}
