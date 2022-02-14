package dialog

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	nopPrompt "github.com/keboola/keboola-as-code/internal/pkg/cli/prompt/nop"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestTemplateInputsDialog_DefaultValue(t *testing.T) {
	t.Parallel()
	configs := configsWithContent()

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
[ ] foo-bar-object-array-1-string    parameters.object.array[1].string    <!-- Lorem ipsum dolor si… -->

`

	// Check default value
	d := templateInputsDialog{prompt: nopPrompt.New(), configs: configs, options: options.New()}
	assert.Equal(t, expected, d.defaultValue())
}

func TestTemplateInputsDialog_DefaultValue_AllInputs(t *testing.T) {
	t.Parallel()
	configs := configsWithContent()

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
[x] foo-bar-object-array-1-string    parameters.object.array[1].string    <!-- Lorem ipsum dolor si… -->

`

	// Check default value
	opts := options.New()
	opts.Set("all-inputs", true)
	d := templateInputsDialog{prompt: nopPrompt.New(), configs: configs, options: opts}
	assert.Equal(t, expected, d.defaultValue())
}

func configsWithContent() []*model.ConfigWithRows {
	configJson := `
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
	rowJson := `
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
	json.MustDecodeString(configJson, configContent)
	json.MustDecodeString(rowJson, rowContent)

	return []*model.ConfigWithRows{
		{
			Config: &model.Config{
				ConfigKey: model.ConfigKey{ComponentId: "keboola.foo.bar", Id: "my-config-1"},
				Name:      "My Config 1",
				Content:   configContent,
			},
		},
		{
			Config: &model.Config{
				ConfigKey: model.ConfigKey{ComponentId: "keboola.foo.bar", Id: "my-config-2"},
				Name:      "My Config 2",
				Content:   orderedmap.New(),
			},
			Rows: []*model.ConfigRow{
				{
					ConfigRowKey: model.ConfigRowKey{ComponentId: "keboola.foo.bar", ConfigId: "my-config-2", Id: "row-1"},
					Name:         "My Row",
					Content:      orderedmap.New(),
				},
				{
					ConfigRowKey: model.ConfigRowKey{ComponentId: "keboola.foo.bar", ConfigId: "my-config-2", Id: "row-2"},
					Name:         "My Row",
					Content:      rowContent,
				},
				{
					ConfigRowKey: model.ConfigRowKey{ComponentId: "keboola.foo.bar", ConfigId: "my-config-2", Id: "row-3"},
					Name:         "My Row",
					Content:      orderedmap.New(),
				},
			},
		},
	}
}
