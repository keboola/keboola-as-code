package input

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type validationTestCase struct {
	description string
	inputs      Inputs
	error       string
}

func TestValidationRules(t *testing.T) {
	t.Parallel()

	cases := []validationTestCase{
		{
			description: "id with a #",
			inputs: Inputs{
				{
					ID:          "input#id",
					Name:        "input",
					Description: "input desc",
					Type:        "string",
					Kind:        "input",
					Default:     "def",
				},
			},
			error: `group 1, step 1, input "input#id": "id" can only contain alphanumeric characters, dots, underscores and dashes`,
		},
		{
			description: "invalid type for kind",
			inputs: Inputs{
				{
					ID:          "input.id",
					Name:        "input",
					Description: "input desc",
					Type:        "int",
					Kind:        "hidden",
					Default:     "def",
				},
			},
			error: "- group 1, step 1, input \"input.id\": \"type\" int is not allowed for the specified kind\n- group 1, step 1, input \"input.id\": \"default\" must match the specified type",
		},
		{
			description: "missing type",
			inputs: Inputs{
				{
					ID:          "input.id",
					Name:        "input",
					Description: "input desc",
					Default:     "def",
					Kind:        "input",
				},
			},
			error: "group 1, step 1, input \"input.id\": \"type\" is a required field",
		},
		{
			description: "invalid rules",
			inputs: Inputs{
				{
					ID:          "input.id",
					Name:        "input",
					Description: "input desc",
					Type:        "int",
					Kind:        "input",
					Rules:       "gtex=5",
					Default:     33,
				},
			},
			error: "group 1, step 1, input \"input.id\": \"rules\" is not valid: undefined validation function 'gtex'",
		},
		{
			description: "invalid if",
			inputs: Inputs{
				{
					ID:          "input.id2",
					Name:        "input",
					Description: "input desc",
					Type:        "string",
					Kind:        "input",
					If:          "1+(2-1>1",
				},
			},
			error: "group 1, step 1, input \"input.id2\": \"showIf\" cannot compile condition:\n- expression: 1+(2-1>1\n- error: Unbalanced parenthesis",
		},
		{
			description: "int default, empty options",
			inputs: Inputs{
				{
					ID:          "input.id",
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
			error: "",
		},
		{
			description: "no default",
			inputs: Inputs{
				{
					ID:          "input.id2",
					Name:        "input",
					Description: "input desc",
					Type:        "string",
					Kind:        "input",
				},
			},
			error: "",
		},
		{
			description: "unexpected options",
			inputs: Inputs{
				{
					ID:          "input.id",
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
			},
			error: "group 1, step 1, input \"input.id\": \"options\" should only be set for select and multiselect kinds",
		},
		{
			description: "empty options",
			inputs: Inputs{
				{
					ID:          "input.id",
					Name:        "input",
					Description: "input desc",
					Type:        "string",
					Kind:        "select",
					Options:     Options{},
				},
			},
			error: "group 1, step 1, input \"input.id\": \"options\" must contain at least one item",
		},
		{
			description: "invalid default value for Select",
			inputs: Inputs{
				{
					ID:          "input.id",
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
			},
			error: "group 1, step 1, input \"input.id\": \"default\" can only contain values from the specified options",
		},
		{
			description: "valid options for Select",
			inputs: Inputs{
				{
					ID:          "input.id",
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
			},
			error: "",
		},
		{
			description: "invalid default value for MultiSelect",
			inputs: Inputs{
				{
					ID:          "input.id",
					Name:        "input",
					Description: "input desc",
					Type:        "string[]",
					Kind:        "multiselect",
					Default:     []any{"a", "d"},
					Options: Options{
						{Value: "a", Label: "A"},
						{Value: "b", Label: "B"},
						{Value: "c", Label: "C"},
					},
				},
			},
			error: "group 1, step 1, input \"input.id\": \"default\" can only contain values from the specified options",
		},
		{
			description: "valid options for MultiSelect",
			inputs: Inputs{
				{
					ID:          "input.id",
					Name:        "input",
					Description: "input desc",
					Type:        "string[]",
					Kind:        "multiselect",
					Default:     []any{"a", "c"},
					Options: Options{
						{Value: "a", Label: "A"},
						{Value: "b", Label: "B"},
						{Value: "c", Label: "C"},
					},
				},
			},
			error: "",
		},
		{
			description: "valid oauth",
			inputs: Inputs{
				{
					ID:          "input.id",
					Name:        "input",
					Description: "input desc",
					Type:        "object",
					Kind:        "oauth",
					ComponentID: "foo.bar",
				},
			},
			error: "",
		},
		{
			description: "missing componentId for oauth kind",
			inputs: Inputs{
				{
					ID:          "input.id",
					Name:        "input",
					Description: "input desc",
					Type:        "object",
					Kind:        "oauth",
				},
			},
			error: "group 1, step 1, input \"input.id\": \"componentId\" is a required field",
		},
		{
			description: "valid oauthAccounts",
			inputs: Inputs{
				{
					ID:           "input.id",
					Name:         "input",
					Description:  "input desc",
					Type:         "object",
					Kind:         "oauthAccounts",
					OauthInputID: "input.other",
				},
				{
					ID:          "input.other",
					Name:        "input",
					Description: "input desc",
					Type:        "object",
					Kind:        "oauth",
					ComponentID: "keboola.ex-instagram",
				},
			},
			error: "",
		},
		{
			description: "invalid \"oauthAccounts\", unsupported component",
			inputs: Inputs{
				{
					ID:           "input.id",
					Name:         "input",
					Description:  "input desc",
					Type:         "object",
					Kind:         "oauthAccounts",
					OauthInputID: "input.other",
				},
				{
					ID:          "input.other",
					Name:        "input",
					Description: "input desc",
					Type:        "object",
					Kind:        "oauth",
					ComponentID: "foo.bar",
				},
			},
			error: "input \"input.id\" (kind=oauthAccounts) is defined for \"foo.bar\" component, but it is not supported",
		},
		{
			description: "missing \"oauthInputId\" for oauthAccounts kind",
			inputs: Inputs{
				{
					ID:          "input.id",
					Name:        "input",
					Description: "input desc",
					Type:        "object",
					Kind:        "oauthAccounts",
				},
			},
			error: "group 1, step 1, input \"input.id\": \"oauthInputId\" is a required field",
		},
		{
			description: "missing referenced input from kind oauthAccounts",
			inputs: Inputs{
				{
					ID:           "input.id",
					Name:         "input",
					Description:  "input desc",
					Type:         "object",
					Kind:         "oauthAccounts",
					OauthInputID: "input.other",
				},
			},
			error: "input \"input.other\" not found, referenced from: input.id",
		},
	}

	stepsGroups := StepsGroups{
		StepsGroup{Description: "group", Required: "all", Steps: []Step{
			{Icon: "common:settings", Name: "Step One", Description: "Description"},
		}},
	}

	// Test all cases
	for _, c := range cases {
		stepsGroups[0].Steps[0].Inputs = c.inputs
		err := stepsGroups.ValidateDefinitions(t.Context())
		if c.error == "" {
			// Expected nil errors.MultiError
			require.NoError(t, err)
		} else {
			require.Error(t, err, c.description)
			assert.Equal(t, c.error, err.Error(), c.description)
		}
	}
}
