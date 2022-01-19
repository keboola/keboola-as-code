package input

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
)

func TestTemplateInputsValidateDefinitions(t *testing.T) {
	t.Parallel()

	// Fail - Id with a dash
	f := file{
		Inputs: []Input{
			{
				Id:          "input-id",
				Name:        "input",
				Description: "input desc",
				Type:        "string",
				Default:     "def",
				Kind:        "input",
			},
		},
	}
	err := f.validate()
	assert.Error(t, err)
	assert.Equal(t, `inputs[0].id can only contain alphanumeric characters, dots and underscores`, err.Error())

	// Fail - type for wrong kind
	f = file{
		Inputs: []Input{
			{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Type:        "int",
				Default:     "def",
				Kind:        "password",
			},
		},
	}
	err = f.validate()
	assert.Error(t, err)
	assert.Equal(t, `- inputs[0].default must be the same type as type or options
- inputs[0].type allowed only for input type`, err.Error())

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
				Default:     33,
				Kind:        "input",
				Rules:       "gtex=5",
			},
		},
	}
	err = f.validate()
	assert.Error(t, err)
	assert.Equal(t, `inputs[0].rules is not valid`, err.Error())

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
	assert.Equal(t, `inputs[0].if is not valid`, err.Error())

	// Success - int Default and empty Options
	f = file{
		Inputs: []Input{
			{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Type:        "int",
				Default:     33,
				Options:     Options{},
				Kind:        "input",
				Rules:       "gte=5",
				If:          "1+(2-1)>1",
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
				Default:     "def",
				Options: Options{
					{Id: "a", Name: "A"},
					{Id: "b", Name: "B"},
				},
				Kind: "input",
			},
		},
	}
	err := f.validate()
	assert.Error(t, err)
	assert.Equal(t, `- inputs[0].type is a required field
- inputs[0].options allowed only for select and multiselect`, err.Error())

	// Fail - empty Options
	f = file{
		Inputs: []Input{
			{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Options:     Options{},
				Kind:        "select",
			},
		},
	}
	err = f.validate()
	assert.Error(t, err)
	assert.Equal(t, `inputs[0].options allowed only for select and multiselect`, err.Error())

	// Fail - Default value missing in Options
	f = file{
		Inputs: []Input{
			{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Default:     "c",
				Options: Options{
					{Id: "a", Name: "A"},
					{Id: "b", Name: "B"},
				},
				Kind: "select",
			},
		},
	}
	err = f.validate()
	assert.Error(t, err)
	assert.Equal(t, `inputs[0].default must be the same type as type or options`, err.Error())

	// Success - with Options
	f = file{
		Inputs: []Input{
			{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Default:     "a",
				Options: Options{
					{Id: "a", Name: "A"},
					{Id: "b", Name: "B"},
				},
				Kind: "select",
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
				Default:     []string{"a", "d"},
				Options: Options{
					{Id: "a", Name: "A"},
					{Id: "b", Name: "B"},
					{Id: "c", Name: "C"},
				},
				Kind: "multiselect",
			},
		},
	}
	err = f.validate()
	assert.Error(t, err)
	assert.Equal(t, `inputs[0].default must be the same type as type or options`, err.Error())

	// Success - Default for MultiOptions
	f = file{
		Inputs: []Input{
			{
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
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(path, inputsJsonNet)))

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
	file, err := fs.ReadFile(Path(), "")
	assert.NoError(t, err)
	assert.Equal(t, testhelper.EscapeWhitespaces(inputsJsonNet), testhelper.EscapeWhitespaces(file.Content))
}

const inputsJsonNet = `{
  inputs: [
    {
      id: "fb.extractor.username",
      name: "Facebook username",
      description: "Facebook username description",
      kind: "input",
      type: "string",
    },
    {
      id: "fb.extractor.password",
      name: "Facebook password",
      description: "Facebook password description",
      kind: "password",
    },
    {
      id: "fb.extractor.options",
      name: "Facebook options",
      description: "Facebook options description",
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

func testInputs() Inputs {
	return Inputs{
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
	}
}
