package dialog

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/umisama/go-regexpcache"

	nopPrompt "github.com/keboola/keboola-as-code/internal/pkg/cli/prompt/nop"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
)

func TestInputsDetailDialog_DefaultValue(t *testing.T) {
	t.Parallel()

	// Check default value
	d := newInputsDetailsDialog(nopPrompt.New(), testInputs())
	actual := d.defaultValue()
	actual = regexpcache.MustCompile(` +\n`).ReplaceAllString(actual, "\n") // trim trailing spaces
	assert.Equal(t, inputsDetailDialogDefaultValue, actual)
}

func TestInputsDetailDialog_Parse_NoChange(t *testing.T) {
	t.Parallel()

	// Parse
	d := newInputsDetailsDialog(nopPrompt.New(), testInputs())
	err := d.parse(inputsDetailDialogDefaultValue)
	assert.NoError(t, err)
	assert.Equal(t, testInputs().all(), d.inputs.all())
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

## Input "bool-confirm" (bool)
name: Bool Confirm
description: Description
kind: confirm
rules:
showIf:
default: true


## Input "string-select" (string)
name: String Select
description:
kind: select
rules:
showIf:
default: id1
options: {"id1":"Op... <!-- invalid options -->

## Input "string-array-multiselect" (string[])
name: String Array
description: Description
kind: multiselect
rules:
showIf:
default: id5, id6 <!-- invalid values -->
options: {"id1":"Option 1","id2":"Option 2","id3":123}  <!-- invalid options -->
`

	expected := `
- line 26: value "{"id1":"Op..." is not valid: unexpected end of JSON input, offset: 13
- line 35: value "{"id1":"Option 1","id2":"Option 2","id3":123}" is not valid: value of key "id3" must be string
- input "string-input": type string is not allowed for the specified kind
- input "string-input": rules is not valid: undefined validation function 'foobar'
- input "string-input": showIf cannot compile condition:
  - expression: [invalid
  - error: Unclosed parameter bracket
- input "string-array-multiselect": default can only contain values from the specified options
`

	// Parse
	d := newInputsDetailsDialog(nopPrompt.New(), testInputs())
	err := d.parse(result)
	assert.Error(t, err)
	assert.Equal(t, strings.Trim(expected, "\n"), err.Error())
}

func testInputs() inputsMap {
	inputs := newInputsMap()
	inputs.add(&template.Input{
		Id:          "string-input",
		Name:        "String Input",
		Description: "Description",
		Type:        input.TypeString,
		Kind:        input.KindInput,
		Default:     "default",
	})
	inputs.add(&template.Input{
		Id:          "string-hidden",
		Name:        "String Hidden",
		Description: "Description",
		Type:        input.TypeString,
		Kind:        input.KindHidden,
	})
	inputs.add(&template.Input{
		Id:          "string-textarea",
		Name:        "String Textarea",
		Description: "Description",
		Type:        input.TypeString,
		Kind:        input.KindTextarea,
	})
	inputs.add(&template.Input{
		Id:          "string-select",
		Name:        "String Select",
		Description: "Description",
		Type:        input.TypeString,
		Kind:        input.KindSelect,
		Default:     "id1",
		Options: input.Options{
			{
				Id:   "id1",
				Name: "Option 1",
			},
			{
				Id:   "id2",
				Name: "Option 2",
			},
		},
	})
	inputs.add(&template.Input{
		Id:          "string-int",
		Name:        "String Double",
		Description: "Description",
		Type:        input.TypeInt,
		Kind:        input.KindInput,
		Default:     123,
	})
	inputs.add(&template.Input{
		Id:          "string-double",
		Name:        "String Double",
		Description: "Description",
		Type:        input.TypeDouble,
		Kind:        input.KindInput,
		Default:     12.34,
	})
	inputs.add(&template.Input{
		Id:          "bool-confirm",
		Name:        "Bool Confirm",
		Description: "Description",
		Type:        input.TypeBool,
		Kind:        input.KindConfirm,
		Default:     true,
	})
	inputs.add(&template.Input{
		Id:          "string-array-multiselect",
		Name:        "String Array",
		Description: "Description",
		Type:        input.TypeStringArray,
		Kind:        input.KindMultiSelect,
		Default:     []interface{}{"id1", "id3"},
		Options: input.Options{
			{
				Id:   "id1",
				Name: "Option 1",
			},
			{
				Id:   "id2",
				Name: "Option 2",
			},
			{
				Id:   "id3",
				Name: "Option 3",
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
     default: id1
     options: {"id1":"Option 1","id2":"Option 2","id3":"Option 3"}

     kind: multiselect
     default: id1, id3
     options: {"id1":"Option 1","id2":"Option 2","id3":"Option 3"}
-->


## Input "string-input" (string)
name: String Input
description: Description
kind: input
rules:
showIf:
default: default

## Input "string-hidden" (string)
name: String Hidden
description: Description
kind: hidden
rules:
showIf:
default:

## Input "string-textarea" (string)
name: String Textarea
description: Description
kind: textarea
rules:
showIf:
default:

## Input "string-select" (string)
name: String Select
description: Description
kind: select
rules:
showIf:
default: id1
options: {"id1":"Option 1","id2":"Option 2"}

## Input "string-int" (int)
name: String Double
description: Description
kind: input
rules:
showIf:
default: 123

## Input "string-double" (double)
name: String Double
description: Description
kind: input
rules:
showIf:
default: 12.34

## Input "bool-confirm" (bool)
name: Bool Confirm
description: Description
kind: confirm
rules:
showIf:
default: true

## Input "string-array-multiselect" (string[])
name: String Array
description: Description
kind: multiselect
rules:
showIf:
default: id1, id3
options: {"id1":"Option 1","id2":"Option 2","id3":"Option 3"}

`
