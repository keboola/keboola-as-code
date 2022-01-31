package dialog

import (
	"context"
	"fmt"
	"strconv"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	useTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/use"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type contextKey string

type useTmplDialogDeps interface {
	Options() *options.Options
	ProjectState(loadOptions loadState.Options) (*project.State, error)
}

type useTmplDialog struct {
	*Dialogs
	deps             useTmplDialogDeps
	loadStateOptions loadState.Options
	out              useTemplate.Options
	context          context.Context        // for input.ValidateUserInput
	inputsValues     map[string]interface{} // for input.Available
}

func (p *Dialogs) AskUseTemplateOptions(inputs *input.Inputs, d useTmplDialogDeps, loadStateOptions loadState.Options) (useTemplate.Options, error) {
	return (&useTmplDialog{
		Dialogs:          p,
		deps:             d,
		loadStateOptions: loadStateOptions,
		context:          context.Background(),
		inputsValues:     make(map[string]interface{}),
	}).ask(inputs)
}

func (d *useTmplDialog) ask(inputs *input.Inputs) (useTemplate.Options, error) {
	// Load state
	projectState, err := d.deps.ProjectState(d.loadStateOptions)
	if err != nil {
		return d.out, err
	}

	// Target branch
	targetBranch, err := d.SelectBranch(d.deps.Options(), projectState.LocalObjects().Branches(), `Select the target branch`)
	if err != nil {
		return d.out, err
	}
	d.out.TargetBranch = targetBranch.BranchKey

	// User inputs
	if err := d.askInputs(inputs); err != nil {
		return d.out, err
	}

	return d.out, nil
}

func (d *useTmplDialog) addInputValue(inputDef input.Input, value interface{}) error {
	if err := inputDef.ValidateUserInput(value, d.context); err != nil {
		return err
	}
	d.context = context.WithValue(d.context, contextKey(inputDef.Id), value)
	d.inputsValues[inputDef.Id] = value
	d.out.Inputs = append(d.out.Inputs, useTemplate.InputValue{Key: inputDef.Id, Value: value})
	return nil
}

func (d *useTmplDialog) askInputs(inputs *input.Inputs) error {
	for _, inputDef := range inputs.All() {
		if !inputDef.Available(d.inputsValues) {
			continue
		}
		if err := d.askInput(inputDef); err != nil {
			return err
		}
	}
	return nil
}

func (d *useTmplDialog) askInput(inputDef input.Input) error {
	switch inputDef.Kind {
	case input.KindInput, input.KindPassword, input.KindTextarea:
		question := &prompt.Question{
			Label:       inputDef.Name,
			Description: inputDef.Description,
			Validator: func(raw interface{}) error {
				value, err := convertStrValue(raw.(string), inputDef.Type)
				if err != nil {
					return err
				}
				return inputDef.ValidateUserInput(value, d.context)
			},
			Hidden: inputDef.Kind == input.KindPassword,
		}
		question.Default, _ = inputDef.Default.(string)
		valueStr, _ := d.Ask(question)
		// Convert string to input type
		value, err := convertStrValue(valueStr, inputDef.Type)
		if err != nil {
			return err
		}
		// Save value
		if err := d.addInputValue(inputDef, value); err != nil {
			return err
		}
	case input.KindConfirm:
		confirm := &prompt.Confirm{
			Label:       inputDef.Name,
			Description: inputDef.Description,
		}
		confirm.Default, _ = inputDef.Default.(bool)
		if err := d.addInputValue(inputDef, d.Confirm(confirm)); err != nil {
			return err
		}
	case input.KindSelect:
		selectPrompt := &prompt.SelectIndex{
			Label:       inputDef.Name,
			Description: inputDef.Description,
			Options:     inputDef.Options.Names(),
			UseDefault:  true,
			Validator: func(val interface{}) error {
				return inputDef.ValidateUserInput(val, d.context)
			},
		}
		if inputDef.Default != nil {
			selectPrompt.Default = inputDef.Options.GetIndexByName(inputDef.Default.(string))
		}
		selectedIndex, _ := d.SelectIndex(selectPrompt)
		if err := d.addInputValue(inputDef, inputDef.Options[selectedIndex].Id); err != nil {
			return err
		}
	case input.KindMultiSelect:
		multiSelect := &prompt.MultiSelectIndex{
			Label:       inputDef.Name,
			Description: inputDef.Description,
			Options:     inputDef.Options.Names(),
			Validator: func(val interface{}) error {
				return inputDef.ValidateUserInput(val, d.context)
			},
		}
		// Default indices
		if inputDef.Default != nil {
			defaultIndices := make([]int, 0)
			for _, o := range inputDef.Default.([]string) {
				defaultIndices = append(defaultIndices, inputDef.Options.GetIndexByName(o))
			}
			multiSelect.Default = defaultIndices
		}
		// Selected values
		selectedIndices, _ := d.MultiSelectIndex(multiSelect)
		selectedValues := make([]string, 0)
		for _, selectedIndex := range selectedIndices {
			selectedValues = append(selectedValues, inputDef.Options[selectedIndex].Id)
		}
		// Save value
		if err := d.addInputValue(inputDef, selectedValues); err != nil {
			return err
		}
	}

	return nil
}

func convertStrValue(value string, targetType string) (interface{}, error) {
	switch targetType {
	case `int`:
		if v, err := strconv.Atoi(value); err == nil {
			return v, nil
		} else {
			return nil, fmt.Errorf(`value "%s" is not integer`, value)
		}
	case `float64`:
		if v, err := strconv.ParseFloat(value, 64); err == nil {
			return v, nil
		} else {
			return nil, fmt.Errorf(`value "%s" is not float`, value)
		}
	case `string`:
		return value, nil
	default:
		panic(fmt.Errorf("unexpected input type \"%s\"", targetType))
	}
}
