package dialog

import (
	"strings"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	nopPrompt "github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/nop"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
)

func TestInputsSelectDialog_DefaultValue(t *testing.T) {
	t.Parallel()
	branch, configs := configsWithContent()

	// Expected default value
	expected := `
<!--
Please define user inputs for the template.
Edit lines below "## Config ..." and "### Row ...".
Do not edit "<field.path>" and lines starting with "#"!

Line format: <mark> <input-id> <field.path> <example>

1. Mark which fields will be user inputs.
[x] "input-id" "field.path"   <<< this field will be user input
[ ] "input-id" "field.path"   <<< this field will be scalar value

2. Modify "<input-id>" if needed.
Allowed characters: a-z, A-Z, 0-9, "-".
-->


## Config "My Config 1" keboola.foo.bar:my-config-1
[x] foo-bar-password  parameters.#password
[ ] foo-bar-bool      parameters.bool      <!-- false -->
[ ] foo-bar-double    parameters.double    <!-- 78.9 -->
[ ] foo-bar-int       parameters.int       <!-- 123 -->
[ ] foo-bar-string    parameters.string    <!-- my string -->
[ ] foo-bar-strings   parameters.strings

### Row "My Row" keboola.foo.bar:my-config-2:row-2
[x] foo-bar-object-array-1-password  parameters.object.array[1].#password
[ ] foo-bar-object-array-1-bool      parameters.object.array[1].bool      <!-- false -->
[ ] foo-bar-object-array-1-double    parameters.object.array[1].double    <!-- 78.9 -->
[ ] foo-bar-object-array-1-int       parameters.object.array[1].int       <!-- 123 -->
[ ] foo-bar-object-array-1-string    parameters.object.array[1].string    <!-- Lorem ipsum dolor si... -->

`

	// Check default value
	components := testapi.MockedComponentsMap()
	dialog, err := newInputsSelectDialog(nopPrompt.New(), false, components, branch, configs, input.NewInputsMap())
	require.NoError(t, err)
	actual := dialog.defaultValue()
	actual = strings.ReplaceAll(actual, "`", "")
	assert.Equal(t, expected, actual)
}

func TestInputsSelectDialog_DefaultValue_AllInputs(t *testing.T) {
	t.Parallel()
	branch, configs := configsWithContent()

	// Expected default value
	expected := `
<!--
Please define user inputs for the template.
Edit lines below "## Config ..." and "### Row ...".
Do not edit "<field.path>" and lines starting with "#"!

Line format: <mark> <input-id> <field.path> <example>

1. Mark which fields will be user inputs.
[x] "input-id" "field.path"   <<< this field will be user input
[ ] "input-id" "field.path"   <<< this field will be scalar value

2. Modify "<input-id>" if needed.
Allowed characters: a-z, A-Z, 0-9, "-".
-->


## Config "My Config 1" keboola.foo.bar:my-config-1
[x] foo-bar-password  parameters.#password
[x] foo-bar-bool      parameters.bool      <!-- false -->
[x] foo-bar-double    parameters.double    <!-- 78.9 -->
[x] foo-bar-int       parameters.int       <!-- 123 -->
[x] foo-bar-string    parameters.string    <!-- my string -->
[x] foo-bar-strings   parameters.strings

### Row "My Row" keboola.foo.bar:my-config-2:row-2
[x] foo-bar-object-array-1-password  parameters.object.array[1].#password
[x] foo-bar-object-array-1-bool      parameters.object.array[1].bool      <!-- false -->
[x] foo-bar-object-array-1-double    parameters.object.array[1].double    <!-- 78.9 -->
[x] foo-bar-object-array-1-int       parameters.object.array[1].int       <!-- 123 -->
[x] foo-bar-object-array-1-string    parameters.object.array[1].string    <!-- Lorem ipsum dolor si... -->

`

	// Check default value
	components := testapi.MockedComponentsMap()
	dialog, err := newInputsSelectDialog(nopPrompt.New(), true, components, branch, configs, input.NewInputsMap())
	require.NoError(t, err)
	actual := dialog.defaultValue()
	actual = strings.ReplaceAll(actual, "`", "")
	assert.Equal(t, expected, actual)
}

func TestInputsSelectDialog_Parse(t *testing.T) {
	t.Parallel()
	branch, configs := configsWithContent()

	result := `
## Config "My Config 1" keboola.foo.bar:my-config-1
[x] foo-bar-password  parameters.#password
[ ] foo-bar-bool      parameters.bool      <!-- false -->
[ ] foo-bar-double    parameters.double    <!-- 78.9 -->
[ ] foo-bar-int       parameters.int       <!-- 123 -->
[ ] foo-bar-string    parameters.string    <!-- my string -->
[ ] foo-bar-strings   parameters.strings

### Row "My Row" keboola.foo.bar:my-config-2:row-2
[x] foo-bar-object-array-1-password  parameters.object.array[1].#password
[ ] foo-bar-object-array-1-bool      parameters.object.array[1].bool      <!-- false -->
[ ] foo-bar-object-array-1-double    parameters.object.array[1].double    <!-- 78.9 -->
[ ] foo-bar-object-array-1-int       parameters.object.array[1].int       <!-- 123 -->
[ ] foo-bar-object-array-1-string    parameters.object.array[1].string    <!-- Lorem ipsum dolor si... -->
`

	// Parse
	components := testapi.MockedComponentsMap()
	dialog, err := newInputsSelectDialog(nopPrompt.New(), false, components, branch, configs, input.NewInputsMap())
	require.NoError(t, err)
	require.NoError(t, dialog.parse(result))

	// Assert inputs definitions
	configKey := model.ConfigKey{BranchID: 123, ComponentID: "keboola.foo.bar", ID: "my-config-1"}
	rowKey := model.ConfigRowKey{BranchID: 123, ComponentID: "keboola.foo.bar", ConfigID: "my-config-2", ID: "row-2"}
	assert.Equal(t, template.Inputs{
		{ID: "foo-bar-password", Type: input.TypeString, Kind: input.KindHidden, Name: "Password"},
		{ID: "foo-bar-object-array-1-password", Type: input.TypeString, Kind: input.KindHidden, Name: "Object Array Password"},
	}, dialog.inputs.All())

	// Assert object inputs
	assert.Equal(t, objectInputsMap{
		configKey: {
			{
				Path:    orderedmap.PathFromStr("parameters.#password"),
				InputID: "foo-bar-password",
			},
		},
		rowKey: {
			{
				Path:    orderedmap.PathFromStr("parameters.object.array[1].#password"),
				InputID: "foo-bar-object-array-1-password",
			},
		},
	}, dialog.objectInputs)
}

func TestInputsSelectDialog_Parse_All(t *testing.T) {
	t.Parallel()
	branch, configs := configsWithContent()

	result := `
## Config "My Config 1" keboola.foo.bar:my-config-1
[x] foo-bar-password  parameters.#password
[x] foo-bar-bool      parameters.bool      <!-- false -->
[x] foo-bar-double    parameters.double    <!-- 78.9 -->
[x] foo-bar-int       parameters.int       <!-- 123 -->
[x] foo-bar-string    parameters.string    <!-- my string -->
[x] foo-bar-strings   parameters.strings

### Row "My Row" keboola.foo.bar:my-config-2:row-2
[x] foo-bar-password  parameters.object.array[1].#password
[x] foo-bar-bool      parameters.object.array[1].bool      <!-- false -->
[x] foo-bar-object-array-1-double    parameters.object.array[1].double    <!-- 78.9 -->
[x] foo-bar-object-array-1-int       parameters.object.array[1].int       <!-- 123 -->
[x] foo-bar-object-array-1-string    parameters.object.array[1].string    <!-- Lorem ipsum dolor si... -->
`

	// Parse
	components := testapi.MockedComponentsMap()
	dialog, err := newInputsSelectDialog(nopPrompt.New(), false, components, branch, configs, input.NewInputsMap())
	require.NoError(t, err)
	require.NoError(t, dialog.parse(result))

	// Assert inputs definitions
	configKey := model.ConfigKey{BranchID: 123, ComponentID: "keboola.foo.bar", ID: "my-config-1"}
	rowKey := model.ConfigRowKey{BranchID: 123, ComponentID: "keboola.foo.bar", ConfigID: "my-config-2", ID: "row-2"}
	assert.Equal(t, template.Inputs{
		{ID: "foo-bar-password", Type: input.TypeString, Kind: input.KindHidden, Name: "Password"},
		{ID: "foo-bar-bool", Type: input.TypeBool, Kind: input.KindConfirm, Default: false, Name: "Bool"},
		{ID: "foo-bar-double", Type: input.TypeDouble, Kind: input.KindInput, Default: 78.9, Name: "Double"},
		{ID: "foo-bar-int", Type: input.TypeInt, Kind: input.KindInput, Default: 123, Name: "Int"},
		{ID: "foo-bar-string", Type: input.TypeString, Kind: input.KindInput, Default: "my string", Name: "String"},
		{
			ID:      "foo-bar-strings",
			Name:    "Strings",
			Type:    input.TypeStringArray,
			Kind:    input.KindMultiSelect,
			Default: []any{"foo", "bar"},
			Options: input.Options{
				{
					Value: "foo",
					Label: "foo",
				},
				{
					Value: "bar",
					Label: "bar",
				},
			},
		},
		{ID: "foo-bar-object-array-1-double", Type: input.TypeDouble, Kind: input.KindInput, Default: 78.9, Name: "Object Array Double"},
		{ID: "foo-bar-object-array-1-int", Type: input.TypeInt, Kind: input.KindInput, Default: 123, Name: "Object Array Int"},
		{ID: "foo-bar-object-array-1-string", Type: input.TypeString, Kind: input.KindInput, Name: "Object Array String", Default: "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore"},
	}, dialog.inputs.All())

	// Assert object inputs
	assert.Equal(t, objectInputsMap{
		configKey: {
			{
				Path:    orderedmap.PathFromStr("parameters.#password"),
				InputID: "foo-bar-password",
			},
			{
				Path:    orderedmap.PathFromStr("parameters.bool"),
				InputID: "foo-bar-bool",
			},
			{
				Path:    orderedmap.PathFromStr("parameters.double"),
				InputID: "foo-bar-double",
			},
			{
				Path:    orderedmap.PathFromStr("parameters.int"),
				InputID: "foo-bar-int",
			},
			{
				Path:    orderedmap.PathFromStr("parameters.string"),
				InputID: "foo-bar-string",
			},
			{
				Path:    orderedmap.PathFromStr("parameters.strings"),
				InputID: "foo-bar-strings",
			},
		},
		rowKey: {
			{
				Path:    orderedmap.PathFromStr("parameters.object.array[1].#password"),
				InputID: "foo-bar-password",
			},
			{
				Path:    orderedmap.PathFromStr("parameters.object.array[1].bool"),
				InputID: "foo-bar-bool",
			},
			{
				Path:    orderedmap.PathFromStr("parameters.object.array[1].double"),
				InputID: "foo-bar-object-array-1-double",
			},
			{
				Path:    orderedmap.PathFromStr("parameters.object.array[1].int"),
				InputID: "foo-bar-object-array-1-int",
			},
			{
				Path:    orderedmap.PathFromStr("parameters.object.array[1].string"),
				InputID: "foo-bar-object-array-1-string",
			},
		},
	}, dialog.objectInputs)
}

func TestInputsSelectDialog_Parse_Errors(t *testing.T) {
	t.Parallel()
	branch, configs := configsWithContent()

	result := `
[x] unexpected-input  parameters.input

## Config "Missing Config" keboola.foo.bar:not-found

## Config "Invalid Config" abc

### Row "Missing Config" keboola.foo.bar:not-found:not-found

### Row "Invalid Config" abc

## Config "My Config 1" keboola.foo.bar:my-config-1
[x] foo-bar-password  parameters.#password
[x] unexpected-input  parameters.input
[+] invalid mark  parameters.input
invalid
[x]
[x] invalid
[ ] invalid
[x] foo-bar-password  parameters.bool

### Row "My Row" keboola.foo.bar:my-config-2:row-2
[ ] foo-bar-object-array-1-password  parameters.object.array[1].#password
`

	// Parse
	components := testapi.MockedComponentsMap()
	dialog, err := newInputsSelectDialog(nopPrompt.New(), false, components, branch, configs, input.NewInputsMap())
	require.NoError(t, err)
	err = dialog.parse(result)

	// Assert
	expected := `
- line 2: expected "## Config ..." or "### Row ...", found "[x] unexpe..."
- line 4: config "keboola.foo.bar:not-found" not found
- line 6: cannot parse config "## Config "Invalid Config" abc"
- line 8: config row "keboola.foo.bar:not-found:not-found" not found
- line 10: cannot parse config row "### Row "Invalid Config" abc"
- line 14: field "parameters.input" not found in the config "branch:123/component:keboola.foo.bar/config:my-config-1"
- line 15: expected "[x] ..." or "[ ] ...", found "[+] invali..."
- line 16: expected "<mark> <input-id> <field.path>", found  "invalid"
- line 17: expected "<mark> <input-id> <field.path>", found  "[x]"
- line 18: expected "<mark> <input-id> <field.path>", found  "[x] invalid"
- line 19: expected "<mark> <input-id> <field.path>", found  "[ ] invalid"
- line 20: input "foo-bar-password" is already defined with "string" type, but "parameters.bool" has type "bool"
`
	require.Error(t, err)
	assert.Equal(t, strings.Trim(expected, "\n"), err.Error())
}

func configsWithContent() (*model.Branch, []*model.ConfigWithRows) {
	configJSON := `
{
  "storage": {
    "foo": "bar"
  },
  "parameters": {
    "string": "my string",
    "#password": "my password",
    "int": 123,
    "double": 78.90,
    "bool": false,
    "strings": ["foo", "bar"]
  }
}
`
	rowJSON := `
{
  "storage": {
    "foo": "bar"
  },
 "parameters": {
    "object": {
      "array": [
        123,
        {
          "string": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore",
          "#password": "my password",
          "int": 123,
          "double": 78.90,
          "bool": false
        }
      ]
    }
  }
}
`
	configContent := orderedmap.New()
	rowContent := orderedmap.New()
	json.MustDecodeString(configJSON, configContent)
	json.MustDecodeString(rowJSON, rowContent)

	branch := &model.Branch{BranchKey: model.BranchKey{ID: 123}}
	configs := []*model.ConfigWithRows{
		{
			Config: &model.Config{
				ConfigKey: model.ConfigKey{BranchID: branch.ID, ComponentID: "keboola.foo.bar", ID: "my-config-1"},
				Name:      "My Config 1",
				Content:   configContent,
			},
		},
		{
			Config: &model.Config{
				ConfigKey: model.ConfigKey{BranchID: branch.ID, ComponentID: "keboola.foo.bar", ID: "my-config-2"},
				Name:      "My Config 2",
				Content:   orderedmap.New(),
			},
			Rows: []*model.ConfigRow{
				{
					ConfigRowKey: model.ConfigRowKey{BranchID: branch.ID, ComponentID: "keboola.foo.bar", ConfigID: "my-config-2", ID: "row-1"},
					Name:         "My Row",
					Content:      orderedmap.New(),
				},
				{
					ConfigRowKey: model.ConfigRowKey{BranchID: branch.ID, ComponentID: "keboola.foo.bar", ConfigID: "my-config-2", ID: "row-2"},
					Name:         "My Row",
					Content:      rowContent,
				},
				{
					ConfigRowKey: model.ConfigRowKey{BranchID: branch.ID, ComponentID: "keboola.foo.bar", ConfigID: "my-config-2", ID: "row-3"},
					Name:         "My Row",
					Content:      orderedmap.New(),
				},
			},
		},
	}

	return branch, configs
}
