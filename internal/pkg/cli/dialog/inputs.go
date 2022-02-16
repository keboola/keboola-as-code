package dialog

import (
	"fmt"
	"sort"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type inputsDialogDeps interface {
	Logger() log.Logger
	Options() *options.Options
	Components() (*model.ComponentsMap, error)
}

// askTemplateInputs - dialog to define user inputs for a new template.
// Used in AskCreateTemplateOpts.
func (p *Dialogs) askTemplateInputs(deps inputsDialogDeps, branch *model.Branch, configs []*model.ConfigWithRows) (objectInputsMap, *template.Inputs, error) {
	// Create empty inputs map
	inputs := newInputsMap()

	// Get components
	components, err := deps.Components()
	if err != nil {
		return nil, nil, err
	}

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

	// Define name/description for each user input.
	if err := newInputsDetailsDialog(p.Prompt, inputs).ask(); err != nil {
		return nil, nil, err
	}

	return objectInputs, inputs.all(), nil
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

func newInputsMap() inputsMap {
	return inputsMap{data: orderedmap.New()}
}

// inputsMap - map of all Inputs by Input.Id.
type inputsMap struct {
	data *orderedmap.OrderedMap
}

func (v inputsMap) add(input *template.Input) {
	v.data.Set(input.Id, input)
}

func (v inputsMap) get(inputId string) (*template.Input, bool) {
	value, found := v.data.Get(inputId)
	if !found {
		return nil, false
	}
	return value.(*template.Input), true
}

func (v inputsMap) ids() []string {
	return v.data.Keys()
}

func (v inputsMap) all() *template.Inputs {
	out := make([]template.Input, v.data.Len())
	i := 0
	for _, key := range v.data.Keys() {
		item, _ := v.data.Get(key)
		out[i] = *(item.(*template.Input))
		i++
	}
	return template.NewInputs().Set(out)
}
