package upgrade

import (
	"reflect"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/search"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
)

type inputsValuesExporter struct {
	logger      *log.LevelWriter
	inputsById  map[string]*input.Input
	foundInputs map[string]bool
	groups      input.StepsGroupsExt
	configs     []*model.ConfigWithRows
}

// ExportInputsValues extracts input values from configuration and rows.
// If the value is found:
//   - the value is set as the default input value
//   - step.Show = true, so it is marked configured in th API and pre-selected in CLI
func ExportInputsValues(logger *log.LevelWriter, projectState *state.State, branch model.BranchKey, instanceId string, groups template.StepsGroups) input.StepsGroupsExt {
	e := inputsValuesExporter{
		logger:      logger,
		inputsById:  make(map[string]*input.Input),
		foundInputs: make(map[string]bool),
		groups:      groups.ToExtended(),
		configs:     search.ConfigsForTemplateInstance(projectState.LocalObjects().ConfigsWithRowsFrom(branch), instanceId),
	}
	_ = e.groups.VisitInputs(func(group *input.StepsGroupExt, step *input.StepExt, input *input.Input) error {
		e.inputsById[input.Id] = input
		return nil
	})
	return e.export()
}

func (e inputsValuesExporter) export() input.StepsGroupsExt {
	e.logger.WriteString(`Exporting values of the template inputs from configs/rows ...`)

	// Export inputs values
	iterateTmplMetadata(
		e.configs,
		func(config *model.Config, idInTemplate model.ConfigId, inputs []model.ConfigInputUsage) {
			for _, inputUsage := range inputs {
				e.addValue(config.Key(), config.Content, inputUsage.Input, inputUsage.JsonKey)
			}
		},
		func(row *model.ConfigRow, idInTemplate model.RowId, inputs []model.RowInputUsage) {
			for _, inputUsage := range inputs {
				e.addValue(row.Key(), row.Content, inputUsage.Input, inputUsage.JsonKey)
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

	e.logger.Writef(`Exported %d inputs values.`, len(e.foundInputs))
	return e.groups
}

func (e inputsValuesExporter) addValue(key model.Key, content *orderedmap.OrderedMap, inputId string, jsonKey string) {
	value, keyFound, _ := content.GetNested(jsonKey)
	if !keyFound {
		// Key not found in the row content
		e.logger.Writef(`Value for input "%s" NOT found in JSON key "%s", in %s`, inputId, jsonKey, key.Desc())
		return
	}
	inputDef, inputFound := e.inputsById[inputId]
	if !inputFound {
		// Input is not found in the template
		e.logger.Writef(`Value for input "%s" found, but type doesn't match, JSON key "%s", in %s`, inputId, jsonKey, key.Desc())
		return
	}
	if err := inputDef.Type.ValidateValue(reflect.ValueOf(value)); err != nil {
		// Value has unexpected type
		return
	}

	// Value has been found
	e.logger.Writef(`Value for input "%s" found in JSON key "%s", in %s`, inputId, jsonKey, key.Desc())
	inputDef.Default = value
	e.foundInputs[inputId] = true
}
