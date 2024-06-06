package dialog

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/umisama/go-regexpcache"

	nopPrompt "github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/nop"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
)

func TestInputsDetailDialog_DefaultValue(t *testing.T) {
	t.Parallel()

	// Check default value
	d := newInputsDetailsDialog(nopPrompt.New(), testInputs(), testStepsGroups())
	actual := d.defaultValue()
	actual = regexpcache.MustCompile(` +\n`).ReplaceAllString(actual, "\n") // trim trailing spaces
	assert.Equal(t, inputsDetailDialogDefaultValue, actual)
}

func TestInputsDetailDialog_Parse_DefaultValue(t *testing.T) {
	t.Parallel()

	// Parse
	d := newInputsDetailsDialog(nopPrompt.New(), testInputs(), testStepsGroups())
	stepGroups, err := d.parse(context.Background(), inputsDetailDialogDefaultValue)
	require.NoError(t, err)
	assert.Equal(t, testInputs().All(), d.inputs.All())

	// Inputs are connected to default step group
	expectedStepGroups := input.StepsGroupsExt{
		&input.StepsGroupExt{
			GroupIndex: 0,
			StepsGroup: input.StepsGroup{Description: "desc", Required: "all"},
			Steps: input.StepsExt{
				{
					GroupIndex: 0,
					StepIndex:  0,
					ID:         "s1",
					Step: input.Step{
						Icon:        "common:settings",
						Name:        "Step One",
						Description: "Description",
						Inputs:      testInputs().All(), // <<<<<<<<<<<<<<<<
					},
				},
			},
		},
		&input.StepsGroupExt{
			GroupIndex: 1,
			StepsGroup: input.StepsGroup{Description: "desc2", Required: "all"},
			Steps: input.StepsExt{
				{
					GroupIndex: 1,
					StepIndex:  0,
					ID:         "s2",
					Step: input.Step{
						Icon:        "common:settings",
						Name:        "Step Two",
						Description: "Description",
					},
				},
				{
					GroupIndex: 1,
					StepIndex:  1,
					ID:         "s3",
					Step: input.Step{
						Icon:        "common:settings",
						Name:        "Step Three",
						Description: "Description",
					},
				},
			},
		},
	}
	assert.Equal(t, expectedStepGroups, stepGroups)
}

func TestInputsDetailDialog_Parse_Errors(t *testing.T) {
	t.Parallel()

	result := `
## Input "string-input" (string)
name: String Input
description: 
kind: confirm <!-- invalid kind -->
rules: foobar <!-- invalid rule -->
showIf: [invalid <!-- invalid condition -->
default:
step: s1

## Input "bool-confirm" (bool)
name: Bool Confirm
description: Description
kind: confirm
rules:
showIf:
default: true
step: s1

## Input "string-select" (string)
name: String Select
description:
kind: select
rules:
showIf:
default: value1
options: {"value1":"La... <!-- invalid options -->
step: sABC <!-- invalid step -->

## Input "string-array-multiselect" (string[])
name: String Array
description: Description
kind: multiselect
rules:
showIf:
default: value5, value6 <!-- invalid values -->
options: {"value1":"Label 1","value2":"Label 2","value3":123}  <!-- invalid options -->
<!-- missing step -->
`

	expected := `
- line 27: value "{"value1":"La..." is not valid: unexpected end of JSON input, offset: 16
- line 28: step "sABC" not found
- line 37: value "{"value1":"Label 1","value2":"Label 2","value3":123}" is not valid: value of key "value3" must be string
- input "string-array-multiselect": "step" is not defined
- input "string-input": "type" string is not allowed for the specified kind
- input "string-input": "rules" is not valid: undefined validation function 'foobar'
- input "string-input": "showIf" cannot compile condition:
  - expression: [invalid
  - error: Unclosed parameter bracket
- input "string-array-multiselect": "default" can only contain values from the specified options
`

	// Parse
	d := newInputsDetailsDialog(nopPrompt.New(), testInputs(), testStepsGroups())
	_, err := d.parse(context.Background(), result)
	require.Error(t, err)
	assert.Equal(t, strings.Trim(expected, "\n"), err.Error())
}

func testStepsGroups() input.StepsGroupsExt {
	return input.StepsGroupsExt{
		&input.StepsGroupExt{
			GroupIndex: 0,
			StepsGroup: input.StepsGroup{Description: "desc", Required: "all"},
			Steps: input.StepsExt{
				{GroupIndex: 0, StepIndex: 0, ID: "s1", Step: input.Step{Icon: "common:settings", Name: "Step One", Description: "Description"}},
			},
		},
		&input.StepsGroupExt{
			GroupIndex: 1,
			StepsGroup: input.StepsGroup{Description: "desc2", Required: "all"},
			Steps: input.StepsExt{
				{GroupIndex: 1, StepIndex: 0, ID: "s2", Step: input.Step{Icon: "common:settings", Name: "Step Two", Description: "Description"}},
				{GroupIndex: 1, StepIndex: 1, ID: "s3", Step: input.Step{Icon: "common:settings", Name: "Step Three", Description: "Description"}},
			},
		},
	}
}

func testInputs() input.InputsMap {
	inputs := input.NewInputsMap()
	inputs.Add(&template.Input{
		ID:          "string-input",
		Name:        "String Input",
		Description: "Description",
		Type:        input.TypeString,
		Kind:        input.KindInput,
		Default:     "default",
	})
	inputs.Add(&template.Input{
		ID:          "string-hidden",
		Name:        "String Hidden",
		Description: "Description",
		Type:        input.TypeString,
		Kind:        input.KindHidden,
	})
	inputs.Add(&template.Input{
		ID:          "string-textarea",
		Name:        "String Textarea",
		Description: "Description",
		Type:        input.TypeString,
		Kind:        input.KindTextarea,
	})
	inputs.Add(&template.Input{
		ID:          "string-select",
		Name:        "String Select",
		Description: "Description",
		Type:        input.TypeString,
		Kind:        input.KindSelect,
		Default:     "value1",
		Options: input.Options{
			{
				Value: "value1",
				Label: "Label 1",
			},
			{
				Value: "value2",
				Label: "Label 2",
			},
		},
	})
	inputs.Add(&template.Input{
		ID:          "string-int",
		Name:        "String Double",
		Description: "Description",
		Type:        input.TypeInt,
		Kind:        input.KindInput,
		Default:     123,
	})
	inputs.Add(&template.Input{
		ID:          "string-double",
		Name:        "String Double",
		Description: "Description",
		Type:        input.TypeDouble,
		Kind:        input.KindInput,
		Default:     12.34,
	})
	inputs.Add(&template.Input{
		ID:          "bool-confirm",
		Name:        "Bool Confirm",
		Description: "Description",
		Type:        input.TypeBool,
		Kind:        input.KindConfirm,
		Default:     true,
	})
	inputs.Add(&template.Input{
		ID:          "string-array-multiselect",
		Name:        "String Array",
		Description: "Description",
		Type:        input.TypeStringArray,
		Kind:        input.KindMultiSelect,
		Default:     []any{"value1", "value3"},
		Options: input.Options{
			{
				Value: "value1",
				Label: "Label 1",
			},
			{
				Value: "value2",
				Label: "Label 2",
			},
			{
				Value: "value3",
				Label: "Label 3",
			},
		},
	})
	return inputs
}

const inputsDetailDialogDefaultValue = `
<!--
Please complete definition of the user inputs.
Edit lines below "## Input ...".
Do not edit lines starting with "#"!

1. Adjust the name, description, ... for each user input.

2. Sort the user inputs - move text blocks.
   User will be asked for inputs in the specified order.

Allowed combinations of input type and kind (visual style):
   string:        text
    input         one line text
    hidden        one line text, characters are masked
    textarea      multi-line text
    select        drop-down list, one option must be selected

   int:           whole number
    input         one line text

   double:        decimal number
    input         one line text

   bool:          true/false
    confirm       yes/no prompt

   string[]:      array of strings
    multiselect   drop-down list, multiple options can be selected

Rules example, see: https://github.com/go-playground/validator/blob/master/README.md
    rules: required,email

ShowIf example, see: https://github.com/Knetic/govaluate/blob/master/MANUAL.md
    showIf: [some-previous-input] == "value"

Options format:
     kind: select
     default: value1
     options: {"value1":"Label 1","value2":"Label 2","value3":"Label 3"}

     kind: multiselect
     default: value1, value3
     options: {"value1":"Label 1","value2":"Label 2","value3":"Label 3"}

Preview of steps and groups you created:
- Group 1: desc
  - Step "s1": Step One - Description
- Group 2: desc2
  - Step "s2": Step Two - Description
  - Step "s3": Step Three - Description

-->


## Input "string-input" (string)
name: String Input
description: Description
kind: input
rules:
showIf:
default: default
step: s1

## Input "string-hidden" (string)
name: String Hidden
description: Description
kind: hidden
rules:
showIf:
default:
step: s1

## Input "string-textarea" (string)
name: String Textarea
description: Description
kind: textarea
rules:
showIf:
default:
step: s1

## Input "string-select" (string)
name: String Select
description: Description
kind: select
rules:
showIf:
default: value1
options: {"value1":"Label 1","value2":"Label 2"}
step: s1

## Input "string-int" (int)
name: String Double
description: Description
kind: input
rules:
showIf:
default: 123
step: s1

## Input "string-double" (double)
name: String Double
description: Description
kind: input
rules:
showIf:
default: 12.34
step: s1

## Input "bool-confirm" (bool)
name: Bool Confirm
description: Description
kind: confirm
rules:
showIf:
default: true
step: s1

## Input "string-array-multiselect" (string[])
name: String Array
description: Description
kind: multiselect
rules:
showIf:
default: value1, value3
options: {"value1":"Label 1","value2":"Label 2","value3":"Label 3"}
step: s1

`
