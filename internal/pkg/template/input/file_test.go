package input

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

func TestTemplateInputsValidateDefinitions(t *testing.T) {
	t.Parallel()

	// Fail - Id with a dash
	f := file{
		Inputs: []Input{
			{
				Id:          "input#id",
				Name:        "input",
				Description: "input desc",
				Type:        "string",
				Kind:        "input",
				Default:     "def",
			},
		},
	}
	err := f.validate()
	assert.Error(t, err)
	assert.Equal(t, `inputs[0].id can only contain alphanumeric characters, dots, underscores and dashes`, err.Error())

	// Fail - type for wrong kind
	f = file{
		Inputs: []Input{
			{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Type:        "int",
				Kind:        "password",
				Default:     "def",
			},
		},
	}
	err = f.validate()
	assert.Error(t, err)
	assert.Equal(t, `- inputs[0].type int is not allowed for the specified kind
- inputs[0].default must match the specified type`, err.Error())

	// Fail - input Kind with missing Type
	f = file{
		Inputs: []Input{
			{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Default:     "def",
				Kind:        "input",
			},
		},
	}
	err = f.validate()
	assert.Error(t, err)
	assert.Equal(t, `inputs[0].type is a required field`, err.Error())

	// Fail - wrong Rules
	f = file{
		Inputs: []Input{
			{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Type:        "int",
				Kind:        "input",
				Rules:       "gtex=5",
				Default:     33,
			},
		},
	}
	err = f.validate()
	assert.Error(t, err)
	assert.Equal(t, `inputs[0].rules is not valid: undefined validation function 'gtex'`, err.Error())

	// Fail - wrong If
	f = file{
		Inputs: []Input{
			{
				Id:          "input.id2",
				Name:        "input",
				Description: "input desc",
				Type:        "string",
				Kind:        "input",
				If:          "1+(2-1>1",
			},
		},
	}
	err = f.validate()
	assert.Error(t, err)
	assert.Equal(t, "inputs[0].if cannot compile condition:\n  - expression: 1+(2-1>1\n  - error: Unbalanced parenthesis", err.Error())

	// Success - int Default and empty Options
	f = file{
		Inputs: []Input{
			{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Type:        "int",
				Kind:        "input",
				Default:     33,
				Rules:       "gte=5",
				If:          "1+(2-1)>1",
				Options:     Options{},
			},
		},
	}
	err = f.validate()
	assert.NoError(t, err)

	// Success - no Default
	f = file{
		Inputs: []Input{
			{
				Id:          "input.id2",
				Name:        "input",
				Description: "input desc",
				Type:        "string",
				Kind:        "input",
			},
		},
	}
	err = f.validate()
	assert.NoError(t, err)
}

func TestTemplateInputsValidateDefinitionsSelect(t *testing.T) {
	t.Parallel()

	// Fail - defined options for wrong Kind
	f := file{
		Inputs: []Input{
			{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Type:        "string",
				Kind:        "input",
				Default:     "def",
				Options: Options{
					{Id: "a", Name: "A"},
					{Id: "b", Name: "B"},
				},
			},
		},
	}
	err := f.validate()
	assert.Error(t, err)
	assert.Equal(t, `inputs[0].options should only be set for select and multiselect kinds`, err.Error())

	// Fail - empty Options
	f = file{
		Inputs: []Input{
			{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Type:        "string",
				Kind:        "select",
				Options:     Options{},
			},
		},
	}
	err = f.validate()
	assert.Error(t, err)
	assert.Equal(t, `inputs[0].options must contain at least one item`, err.Error())

	// Fail - Default value missing in Options
	f = file{
		Inputs: []Input{
			{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Type:        "string",
				Kind:        "select",
				Default:     "c",
				Options: Options{
					{Id: "a", Name: "A"},
					{Id: "b", Name: "B"},
				},
			},
		},
	}
	err = f.validate()
	assert.Error(t, err)
	assert.Equal(t, `inputs[0].default can only contain values from the specified options`, err.Error())

	// Success - with Options
	f = file{
		Inputs: []Input{
			{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Type:        "string",
				Kind:        "select",
				Default:     "a",
				Options: Options{
					{Id: "a", Name: "A"},
					{Id: "b", Name: "B"},
				},
			},
		},
	}
	err = f.validate()
	assert.NoError(t, err)

	// Fail - Default value missing in MultiOptions
	f = file{
		Inputs: []Input{
			{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Type:        "string[]",
				Kind:        "multiselect",
				Default:     []interface{}{"a", "d"},
				Options: Options{
					{Id: "a", Name: "A"},
					{Id: "b", Name: "B"},
					{Id: "c", Name: "C"},
				},
			},
		},
	}
	err = f.validate()
	assert.Error(t, err)
	assert.Equal(t, `inputs[0].default can only contain values from the specified options`, err.Error())

	// Success - Default for MultiOptions
	f = file{
		Inputs: []Input{
			{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Type:        "string[]",
				Kind:        "multiselect",
				Default:     []interface{}{"a", "c"},
				Options: Options{
					{Id: "a", Name: "A"},
					{Id: "b", Name: "B"},
					{Id: "c", Name: "C"},
				},
			},
		},
	}
	err = f.validate()
	assert.NoError(t, err)
}

func TestLoadInputs(t *testing.T) {
	t.Parallel()
	fs := testfs.NewMemoryFs()

	// Write file
	path := Path()
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(path, inputsJsonNet)))

	// Load
	inputs, err := Load(fs)
	assert.NoError(t, err)
	assert.Equal(t, testInputs(), inputs)
}

func TestSaveInputs(t *testing.T) {
	t.Parallel()
	fs := testfs.NewMemoryFs()

	// Save
	assert.NoError(t, testInputs().Save(fs))

	// Load file
	file, err := fs.ReadFile(filesystem.NewFileDef(Path()))
	assert.NoError(t, err)
	assert.Equal(t, testhelper.EscapeWhitespaces(inputsJsonNet), testhelper.EscapeWhitespaces(file.Content))
}

const inputsJsonNet = `{
  inputs: [
    {
      id: "fb.extractor.username",
      name: "Facebook username",
      description: "Facebook username description",
      type: "string",
      kind: "input",
    },
    {
      id: "fb.extractor.password",
      name: "Facebook password",
      description: "Facebook password description",
      type: "string",
      kind: "password",
    },
    {
      id: "fb.extractor.options",
      name: "Facebook options",
      description: "Facebook options description",
      type: "string",
      kind: "select",
      options: [
        {
          id: "a",
          name: "A",
        },
        {
          id: "b",
          name: "B",
        },
      ],
    },
  ],
}
`

func testInputs() *Inputs {
	inputs := NewInputs(nil)
	inputs.Set([]Input{
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
			Type:        "string",
			Kind:        "password",
		},
		{
			Id:          "fb.extractor.options",
			Name:        "Facebook options",
			Description: "Facebook options description",
			Type:        "string",
			Kind:        "select",
			Options: Options{
				{Id: "a", Name: "A"},
				{Id: "b", Name: "B"},
			},
		},
	})
	return inputs
}
