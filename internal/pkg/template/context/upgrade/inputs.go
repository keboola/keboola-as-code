package upgrade

import (
	"context"
	"reflect"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/search"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
)

type loggerFn func(ctx context.Context, template string, args ...any)

type inputsValuesExporter struct {
	loggerFn    loggerFn
	inputsByID  map[string]*input.Input
	foundInputs map[string]bool
	groups      input.StepsGroupsExt
	configs     []*model.ConfigWithRows
}

// ExportInputsValues extracts input values from configuration and rows.
// If the value is found:
//   - the value is set as the default input value
//   - step.Show = true, so it is marked configured in th API and pre-selected in CLI
func ExportInputsValues(ctx context.Context, loggerFn loggerFn, projectState *state.State, branch model.BranchKey, instanceID string, groups template.StepsGroups) input.StepsGroupsExt {
	e := inputsValuesExporter{
		loggerFn:    loggerFn,
		inputsByID:  make(map[string]*input.Input),
		foundInputs: make(map[string]bool),
		groups:      groups.ToExtended(),
		configs:     search.ConfigsForTemplateInstance(projectState.LocalObjects().ConfigsWithRowsFrom(branch), instanceID),
	}
	_ = e.groups.VisitInputs(func(group *input.StepsGroupExt, step *input.StepExt, input *input.Input) error {
		e.inputsByID[input.ID] = input
		return nil
	})
	return e.export(ctx)
}

func (e inputsValuesExporter) export(ctx context.Context) input.StepsGroupsExt {
	e.loggerFn(ctx, `Exporting values of the template inputs from configs/rows ...`)

	// Export inputs values
	iterateTmplMetadata(
		e.configs,
		func(config *model.Config, idInTemplate keboola.ConfigID, inputs []model.ConfigInputUsage) {
			for _, inputUsage := range inputs {
				e.addValue(ctx, config.Key(), config.Content, inputUsage.Input, inputUsage.JSONKey, inputUsage.ObjectKeys)
			}
		},
		func(row *model.ConfigRow, idInTemplate keboola.RowID, inputs []model.RowInputUsage) {
			for _, inputUsage := range inputs {
				e.addValue(ctx, row.Key(), row.Content, inputUsage.Input, inputUsage.JSONKey, inputUsage.ObjectKeys)
			}
		},
	)

	// If at least one input from the step is found, set step as configured
	_ = e.groups.VisitInputs(func(group *input.StepsGroupExt, step *input.StepExt, input *input.Input) error {
		if _, found := e.foundInputs[input.ID]; found {
			step.Show = true
		}
		return nil
	})

	e.loggerFn(ctx, `Exported %d inputs values.`, len(e.foundInputs))
	return e.groups
}

func (e inputsValuesExporter) addValue(ctx context.Context, key model.Key, content *orderedmap.OrderedMap, inputID string, jsonKey string, objectKeys []string) {
	value, keyFound, _ := content.GetNested(jsonKey)
	if !keyFound {
		// Key not found in the row content
		e.loggerFn(ctx, `Value for input "%s" NOT found in JSON key "%s", in %s`, inputID, jsonKey, key.Desc())
		return
	}
	inputDef, inputFound := e.inputsByID[inputID]
	if !inputFound {
		// Input is not found in the template
		e.loggerFn(ctx, `Value for input "%s" found, but type doesn't match, JSON key "%s", in %s`, inputID, jsonKey, key.Desc())
		return
	}

	// Convert ordered map to map
	if inputDef.Type == input.TypeObject {
		value = value.(*orderedmap.OrderedMap).ToMap()
	}

	// If "objectKeys" are not empty, it means that only part of the value/object (only some keys) were generated from the Input.
	if objectKeys != nil {
		if jsonObject, ok := value.(map[string]any); ok {
			// Export only defined keys
			mappedValue := make(map[string]any)
			for _, k := range objectKeys {
				if v, ok := jsonObject[k]; ok {
					mappedValue[k] = v
				}
			}
			value = mappedValue
		} else {
			// Object keys requires a JSON object, skip
			return
		}
	}

	// Validate value
	if err := inputDef.Type.ValidateValue(reflect.ValueOf(value)); err != nil {
		// Value has unexpected type
		return
	}

	// Value has been found
	e.loggerFn(ctx, `Value for input "%s" found in JSON key "%s", in %s`, inputID, jsonKey, key.Desc())
	inputDef.Default = value
	e.foundInputs[inputID] = true
}
