package dialog

import (
	"fmt"
	"sort"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/context/create"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
)

type inputsDialogDeps interface {
	Logger() log.Logger
	Options() *options.Options
	Components() *model.ComponentsMap
}

// askNewTemplateInputs - dialog to define user inputs for a new template.
// Used in AskCreateTemplateOpts.
func (p *Dialogs) askNewTemplateInputs(deps inputsDialogDeps, branch *model.Branch, configs []*model.ConfigWithRows) (objectInputsMap, template.StepsGroups, error) {
	// Create empty inputs map
	inputs := input.NewInputsMap()

	// Get components
	components := deps.Components()

	// Select which config/row fields will be replaced by user input.
	selectAllInputs := deps.Options().GetBool("all-inputs")
	selectDialog, err := newInputsSelectDialog(p.Prompt, selectAllInputs, components, branch, configs, inputs)
	if err != nil {
		return nil, nil, err
	}
	objectInputs, err := selectDialog.ask()
	if err != nil {
		return nil, nil, err
	}

	// Define steps and steps groups for user inputs.
	stepsGroups, err := newStepsDialog(p.Prompt).ask()
	if err != nil {
		return nil, nil, err
	}

	// Define name/description for each user input and add inputs to steps groups.
	stepsGroups, err = newInputsDetailsDialog(p.Prompt, inputs, stepsGroups).ask()
	if err != nil {
		return nil, nil, err
	}

	return objectInputs, stepsGroups.ToValue(), nil
}

type inputFields map[string]input.ObjectField

func (f inputFields) Write(out *strings.Builder) {
	var table []inputFieldLine
	var inputIdMaxLength int
	var fieldPathMaxLength int

	// Convert and get max lengths for padding
	for _, field := range f {
		line := createInputFieldLine(field)
		table = append(table, line)

		if len(line.inputId) > inputIdMaxLength {
			inputIdMaxLength = len(line.inputId)
		}

		fieldPathLength := len(line.fieldPath) + 2
		if fieldPathLength > fieldPathMaxLength {
			fieldPathMaxLength = fieldPathLength
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
		// Field path is escaped, it can contain MarkDown special chars, eg. _ []
		out.WriteString(strings.TrimSpace(fmt.Sprintf(format, line.mark, line.inputId, "`"+line.fieldPath+"`", example)))
		out.WriteString("\n")
	}
}

func createInputFieldLine(field input.ObjectField) inputFieldLine {
	mark := "[ ]"
	if field.Selected {
		mark = "[x]"
	}
	return inputFieldLine{
		mark:      mark,
		inputId:   field.Input.Id,
		fieldPath: field.Path.String(),
		example:   field.Example,
	}
}

type inputFieldLine struct {
	mark      string
	inputId   string
	fieldPath string
	example   string
}

// objectInputsMap - map of inputs used in an object.
type objectInputsMap map[model.Key][]create.InputDef

func (v objectInputsMap) add(objectKey model.Key, inputDef create.InputDef) {
	v[objectKey] = append(v[objectKey], inputDef)
}

func (v objectInputsMap) setTo(configs []create.ConfigDef) {
	for i := range configs {
		c := &configs[i]
		c.Inputs = v[c.Key]
		for j := range c.Rows {
			r := &c.Rows[j]
			r.Inputs = v[r.Key]
		}
	}
}
