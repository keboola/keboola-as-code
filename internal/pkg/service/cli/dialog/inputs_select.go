package dialog

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/template/context/create"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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
		Validator: func(val any) error {
			if err := d.parse(val.(string)); err != nil {
				// Print errors to new line
				return errors.PrefixError(err, "\n")
			}
			return nil
		},
	})
	return d.objectInputs, d.parse(result)
}

func (d *inputsSelectDialog) parse(result string) error {
	d.objectInputs = make(objectInputsMap)

	result = strhelper.StripHTMLComments(result)
	scanner := bufio.NewScanner(strings.NewReader(result))
	errs := errors.NewMultiError()
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
				errs.Append(errors.Errorf(`line %d: cannot parse config "%s"`, lineNum, line))
				invalidObject = true
				continue
			}
			key := model.ConfigKey{BranchID: d.branch.ID, ComponentID: keboola.ComponentID(m[1]), ID: keboola.ConfigID(m[2])}
			if _, found := d.objectFields[key]; !found {
				errs.Append(errors.Errorf(`line %d: config "%s:%s" not found`, lineNum, m[1], m[2]))
				invalidObject = true
				continue
			}
			currentObject = key
			invalidObject = false
		case strings.HasPrefix(line, `### Row`):
			// Row ID definition
			m := regexpcache.MustCompile(` ([a-zA-Z0-9\.\-]+):([a-zA-Z0-9\.\-]+):([a-zA-Z0-9\.\-]+)$`).FindStringSubmatch(line)
			if m == nil {
				errs.Append(errors.Errorf(`line %d: cannot parse config row "%s"`, lineNum, line))
				invalidObject = true
				continue
			}
			key := model.ConfigRowKey{BranchID: d.branch.ID, ComponentID: keboola.ComponentID(m[1]), ConfigID: keboola.ConfigID(m[2]), ID: keboola.RowID(m[3])}
			if _, found := d.objectFields[key]; !found {
				errs.Append(errors.Errorf(`line %d: config row "%s:%s:%s" not found`, lineNum, m[1], m[2], m[3]))
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
				errs.Append(err)
				continue
			}
		default:
			// Expected object definition
			errs.Append(errors.Errorf(`line %d: expected "## Config ..." or "### Row ...", found "%s"`, lineNum, strhelper.Truncate(line, 10, "...")))
			continue
		}
	}

	return errs.ErrorOrNil()
}

func (d *inputsSelectDialog) parseInputLine(objectKey model.Key, line string, lineNum int) error {
	// Get mark
	if len(line) < 3 {
		return errors.Errorf(`line %d: expected "<mark> <input-id> <field.path>", found  "%s"`, lineNum, line)
	}
	mark := strings.TrimSpace(line[0:3])

	// Split to parts
	parts := strings.SplitN(strings.TrimSpace(line[3:]), " ", 2)
	if len(parts) != 2 {
		return errors.Errorf(`line %d: expected "<mark> <input-id> <field.path>", found  "%s"`, lineNum, line)
	}
	inputID := strings.TrimSpace(parts[0])
	fieldPath := strings.Trim(parts[1], " `")

	// Process
	switch mark {
	case "[x]", "[X]":
		// Get all object fields
		objectFields, found := d.objectFields[objectKey]
		if !found {
			return errors.Errorf(`line %d: %s not found`, lineNum, objectKey.Desc())
		}

		// Get field by path
		field, found := objectFields[fieldPath]
		if !found {
			return errors.Errorf(`line %d: field "%s" not found in the %s`, lineNum, fieldPath, objectKey.Desc())
		}

		// Modify input ID, if it has been changed by use.
		field.ID = inputID

		// One input can be used multiple times, but type must match.
		if i, found := d.inputs.Get(field.ID); found {
			if i.Type != field.Type {
				return errors.Errorf(`line %d: input "%s" is already defined with "%s" type, but "%s" has type "%s"`, lineNum, i.ID, i.Type, fieldPath, field.Type)
			}
		}

		// Save definitions
		d.objectInputs.add(objectKey, create.InputDef{Path: field.Path, InputID: field.ID})
		if _, found := d.inputs.Get(field.ID); !found {
			value := field.Input
			d.inputs.Add(&value)
		}
		return nil
	case "[ ]", "[]":
		// scalar value, not user input
		return nil
	default:
		return errors.Errorf(`line %d: expected "[x] ..." or "[ ] ...", found "%s"`, lineNum, strhelper.Truncate(line, 10, "..."))
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
			lines.WriteString(fmt.Sprintf("## Config \"%s\" %s:%s\n", c.Name, c.ComponentID, c.ID))
			fields.Write(&lines)
			lines.WriteString("\n")
		}

		// Rows
		for _, r := range c.Rows {
			fields := d.objectFields[r.ConfigRowKey]
			if len(fields) > 0 {
				lines.WriteString(fmt.Sprintf("### Row \"%s\" %s:%s:%s\n", r.Name, r.ComponentID, r.ConfigID, r.ID))
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
		component, err := d.components.GetOrErr(c.ComponentID)
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
