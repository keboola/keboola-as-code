// nolint: unused
package dialog

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/api/encryptionapi"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

type templateInputsDialog struct {
	prompt         prompt.Prompt
	options        *options.Options
	configs        []*model.ConfigWithRows
	fieldsByObject map[model.Key]inputFields
}

// askTemplateInputs - dialog to define user inputs for a new template.
// Used in AskCreateTemplateOpts.
// Used in AskCreateTemplateOpts.
func (p *Dialogs) askTemplateInputs(opts *options.Options, configs []*model.ConfigWithRows) (input.Inputs, error) {
	return (&templateInputsDialog{prompt: p.Prompt, configs: configs, options: opts}).ask()
}

func (d *templateInputsDialog) ask() (input.Inputs, error) {
	result, _ := d.prompt.Editor("md", &prompt.Question{
		Description: `Please define user inputs.`,
		Default:     d.defaultValue(),
		Validator: func(val interface{}) error {
			if _, err := d.parse(val.(string)); err != nil {
				// Print errors to new line
				return utils.PrefixError("\n", err)
			}
			return nil
		},
	})
	return d.parse(result)
}

func (d *templateInputsDialog) parse(result string) (input.Inputs, error) {
	return input.Inputs{}, nil
}

func (d *templateInputsDialog) defaultValue() string {
	// Detect potential inputs in each config and config row
	d.detectInputs()

	// File header - info for user
	fileHeader := `
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


`

	// Add definitions
	var lines strings.Builder
	lines.WriteString(fileHeader)
	for _, c := range d.configs {
		// Config
		fields := d.fieldsByObject[c.ConfigKey]
		if len(fields) > 0 {
			lines.WriteString(fmt.Sprintf("## Config \"%s\" %s:%s\n", c.Name, c.ComponentId, c.Id))
			fields.Write(&lines)
			lines.WriteString("\n")
		}

		// Rows
		for _, r := range c.Rows {
			fields := d.fieldsByObject[r.ConfigRowKey]
			if len(fields) > 0 {
				lines.WriteString(fmt.Sprintf("### Row \"%s\" %s:%s:%s\n", r.Name, r.ComponentId, r.ConfigId, r.Id))
				fields.Write(&lines)
				lines.WriteString("\n")
			}
		}
	}

	return lines.String()
}

// detectInputs - detects potential inputs in each config and config row.
func (d *templateInputsDialog) detectInputs() {
	d.fieldsByObject = make(map[model.Key]inputFields)
	for _, c := range d.configs {
		d.detectInputsFor(c.ConfigKey, c.ComponentId, c.Content)
		for _, r := range c.Rows {
			d.detectInputsFor(r.ConfigRowKey, c.ComponentId, r.Content)
		}
	}
}

// detectInputsFor config or config row.
func (d *templateInputsDialog) detectInputsFor(objectKey model.Key, componentId model.ComponentId, content *orderedmap.OrderedMap) {
	content.VisitAllRecursive(func(fieldPath orderedmap.Key, value interface{}, parent interface{}) {
		// Root key must be "parameters"
		if len(fieldPath) < 2 || fieldPath.First() != orderedmap.MapStep("parameters") {
			return
		}

		// Must be object field
		fieldKey, isObjectField := fieldPath.Last().(orderedmap.MapStep)
		if !isObjectField {
			return
		}

		isSecret := encryptionapi.IsKeyToEncrypt(string(fieldKey))

		// Detect type, kind and default value
		var inputType input.Type
		var inputKind input.Kind
		var inputOptions input.Options
		var defaultValue interface{}
		valRef := reflect.ValueOf(value)
		switch valRef.Kind() {
		case reflect.String:
			inputType = input.TypeString
			if isSecret {
				inputKind = input.KindHidden
			} else {
				inputKind = input.KindInput
			}

			// Use as default value, if it is not a secret
			if !isSecret && len(value.(string)) > 0 {
				defaultValue = value
			}
		case reflect.Int:
			inputType = input.TypeInt
			inputKind = input.KindInput
			if !isSecret && value.(int) != 0 {
				defaultValue = value
			}
		case reflect.Float64:
			inputType = input.TypeDouble
			inputKind = input.KindInput
			if !isSecret && value.(float64) != 0.0 {
				defaultValue = value
			}
		case reflect.Bool:
			inputType = input.TypeBool
			inputKind = input.KindConfirm
			defaultValue = value
		case reflect.Slice:
			inputType = input.TypeStringArray
			inputKind = input.KindMultiSelect
			// Each element must be string
			for i := 0; i < valRef.Len(); i++ {
				item := valRef.Index(i)
				// Unwrap interface
				if item.Type().Kind() == reflect.Interface {
					item = item.Elem()
				}
				// Check item type
				if itemKind := item.Kind(); itemKind != reflect.String {
					// Value is not array of strings
					return
				}
				inputOptions = append(inputOptions, input.Option{
					Id:   item.String(),
					Name: item.String(),
				})
			}
			if !isSecret && valRef.Len() > 0 {
				defaultValue = value
			}
		default:
			return
		}

		// Example
		example := ""
		if !isSecret {
			example = strhelper.Truncate(cast.ToString(value), 20, "…")
		}

		// Add
		d.addInputForField(objectKey, fieldPath, example, input.Input{
			Id:      utils.NormalizeName(componentId.WithoutVendor() + "-" + fieldPath[1:].String()),
			Type:    inputType,
			Kind:    inputKind,
			Default: defaultValue,
			Options: inputOptions,
		})
	})
}

func (d *templateInputsDialog) addInputForField(objectKey model.Key, path orderedmap.Key, example string, i input.Input) {
	if d.fieldsByObject[objectKey] == nil {
		d.fieldsByObject[objectKey] = make(map[string]inputField)
	}

	selected := i.Kind == input.KindHidden || d.options.GetBool("all-inputs")
	d.fieldsByObject[objectKey][path.String()] = inputField{path: path, example: example, input: i, selected: selected}
}

type inputFields map[string]inputField

func (f inputFields) Write(out *strings.Builder) {
	var table []inputFieldLine
	var inputIdMaxLength int
	var fieldPathMaxLength int

	// Convert and get max lengths for padding
	for _, field := range f {
		line := field.Line()
		table = append(table, line)

		if len(line.inputId) > inputIdMaxLength {
			inputIdMaxLength = len(line.inputId)
		}

		if len(line.fieldPath) > fieldPathMaxLength {
			fieldPathMaxLength = len(line.fieldPath)
		}
	}

	// Sort by field path
	sort.SliceStable(table, func(i, j int) bool {
		return table[i].fieldPath < table[j].fieldPath
	})

	// Format with padding
	format := fmt.Sprintf("%%s %%-%ds  %%-%ds %%s", inputIdMaxLength, fieldPathMaxLength)

	// Write
	for _, line := range table {
		example := ""
		if len(line.example) > 0 {
			example = "<!-- " + line.example + " -->"
		}
		out.WriteString(strings.TrimSpace(fmt.Sprintf(format, line.mark, line.inputId, line.fieldPath, example)))
		out.WriteString("\n")
	}
}

type inputField struct {
	path     orderedmap.Key
	example  string
	input    input.Input
	selected bool
}

func (f inputField) Line() inputFieldLine {
	mark := "[ ]"
	if f.selected {
		mark = "[x]"
	}

	return inputFieldLine{
		mark:      mark,
		inputId:   f.input.Id,
		fieldPath: f.path.String(),
		example:   f.example,
	}
}

type inputFieldLine struct {
	mark      string
	inputId   string
	fieldPath string
	example   string
}
