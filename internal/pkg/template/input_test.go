package template

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTemplateInput(t *testing.T) {
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
	err := inputs.Validate()
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
	err = inputs.Validate()
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
	err = inputs.Validate()
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
	err = inputs.Validate()
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
	err = inputs.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `failed "template-input-options"`)

	// Success - with Options
	inputs = Inputs{{
		Id:          "input.id",
		Name:        "input",
		Description: "input desc",
		Default:     "a",
		Options:     []Option{"a", "b"},
		Kind:        "select",
	}}
	err = inputs.Validate()
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
	}}
	err = inputs.Validate()
	assert.NoError(t, err)

	// Success - no Default
	inputs = Inputs{{
		Id:          "input.id2",
		Name:        "input",
		Description: "input desc",
		Type:        "string",
		Kind:        "input",
	}}
	err = inputs.Validate()
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

func TestCheckTypeAgainstKind(t *testing.T) {
	t.Parallel()

	// Confirm Kind
	assert.Error(t, checkTypeAgainstKind("string", "confirm"))
	assert.NoError(t, checkTypeAgainstKind(true, "confirm"))

	// Password Kind
	assert.Error(t, checkTypeAgainstKind(123, "password"))
	assert.NoError(t, checkTypeAgainstKind("string", "password"))

	// Textarea Kind
	assert.Error(t, checkTypeAgainstKind(false, "textarea"))
	assert.NoError(t, checkTypeAgainstKind("string", "textarea"))
}
