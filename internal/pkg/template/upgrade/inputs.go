package upgrade

import (
	"reflect"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/search"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type inputsValuesExporter struct {
	inputsById  map[string]*input.Input
	foundInputs map[string]bool
	groups      input.StepsGroupsExt
	configs     []*model.ConfigWithRows
}

// ExportInputsValues extracts input values from configuration and rows.
// If the value is found:
//   - the value is set as the default input value
//   - step.Show = true, so it is marked configured in th API and pre-selected in CLI
func ExportInputsValues(projectState *state.State, branch model.BranchKey, instanceId string, groups template.StepsGroups) input.StepsGroupsExt {
	e := inputsValuesExporter{
		inputsById:  make(map[string]*input.Input),
		foundInputs: make(map[string]bool),
		groups:      groups.ToExtended(),
		configs:     search.ConfigsForTemplateInstance(projectState.RemoteObjects().ConfigsWithRowsFrom(branch), instanceId),
	}
	_ = e.groups.VisitInputs(func(group *input.StepsGroupExt, step *input.StepExt, input *input.Input) error {
		e.inputsById[input.Id] = input
		return nil
	})
	return e.export()
}

func (e inputsValuesExporter) export() input.StepsGroupsExt {
	// Export inputs values
	iterateTmplMetadata(
		e.configs,
		func(config *model.Config, idInTemplate model.ConfigId, inputs []model.ConfigInputUsage) {
			for _, inputUsage := range inputs {
				e.addValue(config.Content, inputUsage.Input, inputUsage.JsonKey)
			}
		},
		func(row *model.ConfigRow, idInTemplate model.RowId, inputs []model.RowInputUsage) {
			for _, inputUsage := range inputs {
				e.addValue(row.Content, inputUsage.Input, inputUsage.JsonKey)
			}
		},
	)

	// If at least one input from the step is found, set step as configured
	_ = e.groups.VisitInputs(func(group *input.StepsGroupExt, step *input.StepExt, input *input.Input) error {
		if _, found := e.foundInputs[input.Id]; found {
			step.Show = true
		}
		return nil
	})

	return e.groups
}

func (e inputsValuesExporter) addValue(content *orderedmap.OrderedMap, inputId string, jsonKey string) {
	value, keyFound, _ := content.GetNested(jsonKey)
	if !keyFound {
		// Key not found in the row content
		return
	}
	inputDef, inputFound := e.inputsById[inputId]
	if !inputFound {
		// Input is not found in the template
		return
	}
	if err := inputDef.Type.ValidateValue(reflect.ValueOf(value)); err != nil {
		// Value has unexpected type
		return
	}
	inputDef.Default = value
	e.foundInputs[inputId] = true
}
