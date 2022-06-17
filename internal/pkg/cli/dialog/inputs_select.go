package dialog

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template/create"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// inputsSelectDialog to select which config/row fields will be replaced by user input.
type inputsSelectDialog struct {
	prompt       prompt.Prompt
	selectAll    bool
	components   *model.ComponentsMap
	branch       *model.Branch
	configs      []*model.ConfigWithRows
	inputs       input.InputsMap
	objectFields map[model.Key]inputFields
	objectInputs objectInputsMap
}

func newInputsSelectDialog(prompt prompt.Prompt, selectAll bool, components *model.ComponentsMap, branch *model.Branch, configs []*model.ConfigWithRows, inputs input.InputsMap) (*inputsSelectDialog, error) {
	d := &inputsSelectDialog{prompt: prompt, selectAll: selectAll, components: components, inputs: inputs, branch: branch, configs: configs}
	return d, d.detectInputs()
}

func (d *inputsSelectDialog) ask() (objectInputsMap, error) {
	result, _ := d.prompt.Editor("md", &prompt.Question{
		Description: `Please select which fields in the configurations should be user inputs.`,
		Default:     d.defaultValue(),
		Validator: func(val interface{}) error {
			if err := d.parse(val.(string)); err != nil {
				// Print errors to new line
				return utils.PrefixError("\n", err)
			}
			return nil
		},
	})
	return d.objectInputs, d.parse(result)
}

func (d *inputsSelectDialog) parse(result string) error {
	d.objectInputs = make(objectInputsMap)

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
			key := model.ConfigKey{BranchId: d.branch.Id, ComponentId: storageapi.ComponentID(m[1]), Id: storageapi.ConfigID(m[2])}
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
			key := model.ConfigRowKey{BranchId: d.branch.Id, ComponentId: storageapi.ComponentID(m[1]), ConfigId: storageapi.ConfigID(m[2]), Id: storageapi.RowID(m[3])}
			if _, found := d.objectFields[key]; !found {
				errors.Append(fmt.Errorf(`line %d: config row "%s:%s:%s" not found`, lineNum, m[1], m[2], m[3]))
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
			errors.Append(fmt.Errorf(`line %d: expected "## Config ..." or "### Row ...", found "%s"`, lineNum, strhelper.Truncate(line, 10, "...")))
			continue
		}
	}

	return errors.ErrorOrNil()
}

func (d *inputsSelectDialog) parseInputLine(objectKey model.Key, line string, lineNum int) error {
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
	fieldPath := strings.Trim(parts[1], " `")

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
		field.Input.Id = inputId

		// One input can be used multiple times, but type must match.
		if i, found := d.inputs.Get(field.Input.Id); found {
			if i.Type != field.Input.Type {
				return fmt.Errorf(`line %d: input "%s" is already defined with "%s" type, but "%s" has type "%s"`, lineNum, i.Id, i.Type, fieldPath, field.Input.Type)
			}
		}

		// Save definitions
		d.objectInputs.add(objectKey, create.InputDef{Path: field.Path, InputId: field.Input.Id})
		if _, found := d.inputs.Get(field.Input.Id); !found {
			value := field.Input
			d.inputs.Add(&value)
		}
		return nil
	case mark == "[ ]" || mark == "[]":
		// scalar value, not user input
		return nil
	default:
		return fmt.Errorf(`line %d: expected "[x] ..." or "[ ] ...", found "%s"`, lineNum, strhelper.Truncate(line, 10, "..."))
	}
}

func (d *inputsSelectDialog) defaultValue() string {
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
func (d *inputsSelectDialog) detectInputs() error {
	d.objectFields = make(map[model.Key]inputFields)
	for _, c := range d.configs {
		// Get component
		component, err := d.components.Get(c.ComponentKey())
		if err != nil {
			return err
		}

		// Skip special component
		if component.IsTransformation() || component.IsOrchestrator() || component.IsScheduler() || component.IsSharedCode() {
			continue
		}

		// Find user inputs in config and rows
		for _, item := range input.Find(c.ConfigKey, component, c.Content) {
			d.addInputForField(item)
		}
		for _, r := range c.Rows {
			for _, item := range input.Find(r.ConfigRowKey, component, r.Content) {
				d.addInputForField(item)
			}
		}
	}
	return nil
}

func (d *inputsSelectDialog) addInputForField(field input.ObjectField) {
	if d.selectAll {
		field.Selected = true
	}
	if d.objectFields[field.ObjectKey] == nil {
		d.objectFields[field.ObjectKey] = make(inputFields)
	}
	d.objectFields[field.ObjectKey][field.Path.String()] = field
}
