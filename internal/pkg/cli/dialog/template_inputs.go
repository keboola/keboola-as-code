package dialog

import (
	"bufio"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"

	"github.com/spf13/cast"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/api/encryptionapi"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

type templateInputsDialog struct {
	prompt       prompt.Prompt
	options      *options.Options
	branch       *model.Branch
	configs      []*model.ConfigWithRows
	objectFields map[model.Key]inputFields
	objectInputs objectInputsMap
	inputs       inputsMap
}

// askTemplateInputs - dialog to define user inputs for a new template.
// Used in AskCreateTemplateOpts.
func (p *Dialogs) askTemplateInputs(opts *options.Options, branch *model.Branch, configs []*model.ConfigWithRows) (objectInputsMap, *template.Inputs, error) {
	return newTemplateInputsDialog(p.Prompt, opts, branch, configs).ask()
}

func newTemplateInputsDialog(prompt prompt.Prompt, opts *options.Options, branch *model.Branch, configs []*model.ConfigWithRows) *templateInputsDialog {
	d := &templateInputsDialog{prompt: prompt, options: opts, branch: branch, configs: configs}
	d.detectInputs()
	return d
}

func (d *templateInputsDialog) ask() (objectInputsMap, *template.Inputs, error) {
	result, _ := d.prompt.Editor("md", &prompt.Question{
		Description: `Please define user inputs.`,
		Default:     d.defaultValue(),
		Validator: func(val interface{}) error {
			if err := d.parse(val.(string)); err != nil {
				// Print errors to new line
				return utils.PrefixError("\n", err)
			}
			return nil
		},
	})
	if err := d.parse(result); err != nil {
		return nil, nil, err
	}
	return d.objectInputs, d.inputs.all(), nil
}

func (d *templateInputsDialog) parse(result string) error {
	d.objectInputs = make(objectInputsMap)
	d.inputs = newAllInputsMap()

	result = strhelper.StripHtmlComments(result)
	scanner := bufio.NewScanner(strings.NewReader(result))
	errors := utils.NewMultiError()
	lineNum := 0

	var currentObject model.Key
	var invalidObject bool

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines
		if len(line) == 0 {
			continue
		}

		// Parse line
		switch {
		case strings.HasPrefix(line, `## Config`):
			// Config ID definition
			m := regexpcache.MustCompile(` ([a-zA-Z0-9\.\-]+):([a-zA-Z0-9\.\-]+)$`).FindStringSubmatch(line)
			if m == nil {
				errors.Append(fmt.Errorf(`line %d: cannot parse config "%s"`, lineNum, line))
				invalidObject = true
				continue
			}
			key := model.ConfigKey{BranchId: d.branch.Id, ComponentId: model.ComponentId(m[1]), Id: model.ConfigId(m[2])}
			if _, found := d.objectFields[key]; !found {
				errors.Append(fmt.Errorf(`line %d: config "%s:%s" not found`, lineNum, m[1], m[2]))
				invalidObject = true
				continue
			}
			currentObject = key
			invalidObject = false
		case strings.HasPrefix(line, `### Row`):
			// Row ID definition
			m := regexpcache.MustCompile(` ([a-zA-Z0-9\.\-]+):([a-zA-Z0-9\.\-]+):([a-zA-Z0-9\.\-]+)$`).FindStringSubmatch(line)
			if m == nil {
				errors.Append(fmt.Errorf(`line %d: cannot parse config row "%s"`, lineNum, line))
				invalidObject = true
				continue
			}
			key := model.ConfigRowKey{BranchId: d.branch.Id, ComponentId: model.ComponentId(m[1]), ConfigId: model.ConfigId(m[2]), Id: model.RowId(m[3])}
			if _, found := d.objectFields[key]; !found {
				errors.Append(fmt.Errorf(`line %d: config row "%s:%s:%s" not found`, lineNum, m[1], m[2], m[2]))
				invalidObject = true
				continue
			}
			currentObject = key
			invalidObject = false
		case invalidObject:
			// Skip lines after invalid object definition
		case currentObject != nil:
			// Input definition must be after some Config/Row definition (currentObject is set).
			if err := d.parseInputLine(currentObject, line, lineNum); err != nil {
				errors.Append(err)
				continue
			}
		default:
			// Expected object definition
			errors.Append(fmt.Errorf(`line %d: expected "## Config …" or "### Row …", found "%s"`, lineNum, strhelper.Truncate(line, 10, "…")))
			continue
		}
	}

	return errors.ErrorOrNil()
}

func (d *templateInputsDialog) parseInputLine(objectKey model.Key, line string, lineNum int) error {
	// Get mark
	if len(line) < 3 {
		return fmt.Errorf(`line %d: expected "<mark> <input-id> <field.path>", found  "%s"`, lineNum, line)
	}
	mark := strings.TrimSpace(line[0:3])

	// Split to parts
	parts := strings.SplitN(strings.TrimSpace(line[3:]), " ", 2)
	if len(parts) != 2 {
		return fmt.Errorf(`line %d: expected "<mark> <input-id> <field.path>", found  "%s"`, lineNum, line)
	}
	inputId := strings.TrimSpace(parts[0])
	fieldPath := strings.TrimSpace(parts[1])

	// Process
	switch {
	case mark == "[x]" || mark == "[X]":
		// Get all object fields
		objectFields, found := d.objectFields[objectKey]
		if !found {
			return fmt.Errorf(`line %d: %s not found`, lineNum, objectKey.Desc())
		}

		// Get field by path
		field, found := objectFields[fieldPath]
		if !found {
			return fmt.Errorf(`line %d: field "%s" not found in the %s`, lineNum, fieldPath, objectKey.Desc())
		}

		// Modify input ID, if it has been changed by use.
		field.input.Id = inputId

		// One input can be used multiple times, but type must match.
		if i, found := d.inputs.get(field.input.Id); found {
			if i.Type != field.input.Type {
				return fmt.Errorf(`line %d: input "%s" is already defined with "%s" type, but "%s" has type "%s"`, lineNum, i.Id, i.Type, fieldPath, field.input.Type)
			}
		}

		// Save definitions
		d.inputs.add(field.input)
		d.objectInputs.add(objectKey, template.InputDef{Path: field.path, InputId: field.input.Id})
		return nil
	case mark == "[ ]" || mark == "[]":
		// scalar value, not user input
		return nil
	default:
		return fmt.Errorf(`line %d: expected "[x] …" or "[ ] …", found "%s"`, lineNum, strhelper.Truncate(line, 10, "…"))
	}
}

func (d *templateInputsDialog) defaultValue() string {
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
		fields := d.objectFields[c.ConfigKey]
		if len(fields) > 0 {
			lines.WriteString(fmt.Sprintf("## Config \"%s\" %s:%s\n", c.Name, c.ComponentId, c.Id))
			fields.Write(&lines)
			lines.WriteString("\n")
		}

		// Rows
		for _, r := range c.Rows {
			fields := d.objectFields[r.ConfigRowKey]
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
	d.objectFields = make(map[model.Key]inputFields)
	for _, c := range d.configs {
		d.detectInputsFor(c.ConfigKey, c.ComponentId, c.Content)
		for _, r := range c.Rows {
			d.detectInputsFor(r.ConfigRowKey, r.ComponentId, r.Content)
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
			valueFloat := value.(float64)
			isWholeNumber := math.Trunc(valueFloat) == valueFloat
			if isWholeNumber {
				// Whole number? Use TypeInt.
				// All numeric values from a JSON are float64.
				inputType = input.TypeInt
				inputKind = input.KindInput
				if !isSecret && valueFloat != 0.0 {
					defaultValue = int(valueFloat)
				}
			} else {
				inputType = input.TypeDouble
				inputKind = input.KindInput
				if !isSecret && valueFloat != 0.0 {
					defaultValue = value
				}
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
	if d.objectFields[objectKey] == nil {
		d.objectFields[objectKey] = make(map[string]inputField)
	}

	selected := i.Kind == input.KindHidden || d.options.GetBool("all-inputs")
	d.objectFields[objectKey][path.String()] = inputField{path: path, example: example, input: i, selected: selected}
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

// objectInputsMap - map of inputs used in an object.
type objectInputsMap map[model.Key][]template.InputDef

func (v objectInputsMap) add(objectKey model.Key, inputDef template.InputDef) {
	v[objectKey] = append(v[objectKey], inputDef)
}

func (v objectInputsMap) setTo(configs []template.ConfigDef) {
	for i := range configs {
		c := &configs[i]
		c.Inputs = v[c.Key]
		for j := range c.Rows {
			r := &c.Rows[j]
			r.Inputs = v[r.Key]
		}
	}
}

func newAllInputsMap() inputsMap {
	return inputsMap{data: orderedmap.New()}
}

// inputsMap - map of all Inputs by Input.Id.
type inputsMap struct {
	data *orderedmap.OrderedMap
}

func (v inputsMap) add(input template.Input) {
	v.data.Set(input.Id, input)
}

func (v inputsMap) get(inputId string) (template.Input, bool) {
	value, found := v.data.Get(inputId)
	if !found {
		return template.Input{}, false
	}
	return value.(template.Input), true
}

func (v inputsMap) all() *template.Inputs {
	out := make([]template.Input, v.data.Len())
	i := 0
	for _, key := range v.data.Keys() {
		item, _ := v.data.Get(key)
		out[i] = item.(template.Input)
		i++
	}
	return template.NewInputs().Set(out)
}
