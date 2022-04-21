package input

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type validationTestCase struct {
	description string
	input       Input
	error       string
}

func TestValidationRules(t *testing.T) {
	t.Parallel()

	cases := []validationTestCase{
		{
			description: "id with a #",
			input: Input{
				Id:          "input#id",
				Name:        "input",
				Description: "input desc",
				Type:        "string",
				Kind:        "input",
				Default:     "def",
			},
			error: `[0].steps[0].inputs[0].id can only contain alphanumeric characters, dots, underscores and dashes`,
		},
		{
			description: "invalid type for kind",
			input: Input{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Type:        "int",
				Kind:        "hidden",
				Default:     "def",
			},
			error: "- [0].steps[0].inputs[0].type int is not allowed for the specified kind\n- [0].steps[0].inputs[0].default must match the specified type",
		},
		{
			description: "missing type",
			input: Input{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Default:     "def",
				Kind:        "input",
			},
			error: "[0].steps[0].inputs[0].type is a required field",
		},
		{
			description: "invalid rules",
			input: Input{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Type:        "int",
				Kind:        "input",
				Rules:       "gtex=5",
				Default:     33,
			},
			error: "[0].steps[0].inputs[0].rules is not valid: undefined validation function 'gtex'",
		},
		{
			description: "invalid if",
			input: Input{
				Id:          "input.id2",
				Name:        "input",
				Description: "input desc",
				Type:        "string",
				Kind:        "input",
				If:          "1+(2-1>1",
			},
			error: "[0].steps[0].inputs[0].showIf cannot compile condition:\n  - expression: 1+(2-1>1\n  - error: Unbalanced parenthesis",
		},
		{
			description: "int default, empty options",
			input: Input{
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
			error: "",
		},
		{
			description: "no default",
			input: Input{
				Id:          "input.id2",
				Name:        "input",
				Description: "input desc",
				Type:        "string",
				Kind:        "input",
			},
			error: "",
		},
		{
			description: "unexpected options",
			input: Input{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Type:        "string",
				Kind:        "input",
				Default:     "def",
				Options: Options{
					{Value: "a", Label: "A"},
					{Value: "b", Label: "B"},
				},
			},
			error: "[0].steps[0].inputs[0].options should only be set for select and multiselect kinds",
		},
		{
			description: "empty options",
			input: Input{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Type:        "string",
				Kind:        "select",
				Options:     Options{},
			},
			error: "[0].steps[0].inputs[0].options must contain at least one item",
		},
		{
			description: "invalid default value for Select",
			input: Input{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Type:        "string",
				Kind:        "select",
				Default:     "c",
				Options: Options{
					{Value: "a", Label: "A"},
					{Value: "b", Label: "B"},
				},
			},
			error: "[0].steps[0].inputs[0].default can only contain values from the specified options",
		},
		{
			description: "valid options for Select",
			input: Input{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Type:        "string",
				Kind:        "select",
				Default:     "a",
				Options: Options{
					{Value: "a", Label: "A"},
					{Value: "b", Label: "B"},
				},
			},
			error: "",
		},
		{
			description: "invalid default value for MultiSelect",
			input: Input{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Type:        "string[]",
				Kind:        "multiselect",
				Default:     []interface{}{"a", "d"},
				Options: Options{
					{Value: "a", Label: "A"},
					{Value: "b", Label: "B"},
					{Value: "c", Label: "C"},
				},
			},
			error: "[0].steps[0].inputs[0].default can only contain values from the specified options",
		},
		{
			description: "valid options for MultiSelect",
			input: Input{
				Id:          "input.id",
				Name:        "input",
				Description: "input desc",
				Type:        "string[]",
				Kind:        "multiselect",
				Default:     []interface{}{"a", "c"},
				Options: Options{
					{Value: "a", Label: "A"},
					{Value: "b", Label: "B"},
					{Value: "c", Label: "C"},
				},
			},
			error: "",
		},
	}

	stepsGroups := StepsGroups{
		&StepsGroup{Description: "group", Required: "all", Steps: []*Step{
			{Icon: "common:settings", Name: "Step One", Description: "Description"},
		}},
	}

	// Test all cases
	for _, c := range cases {
		stepsGroups[0].Steps[0].Inputs = Inputs{c.input}
		err := stepsGroups.Validate()
		if c.error == "" {
			// Expected nil *utils.MultiError
			assert.Nil(t, err, c.description)
		} else {
			assert.Error(t, err, c.description)
			assert.Equal(t, c.error, err.Error(), c.description)
		}
	}
}
