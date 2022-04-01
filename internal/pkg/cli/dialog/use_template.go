package dialog

import (
	"context"
	"fmt"
	"os"

	"github.com/AlecAivazis/survey/v2"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	useTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/use"
)

const inputsFileFlag = "inputs-file"

type contextKey string

type useTmplDialog struct {
	*Dialogs
	projectState *project.State
	options      *options.Options
	inputsFile   map[string]interface{} // inputs values loaded from a file specified by inputsFileFlag
	out          useTemplate.Options
	context      context.Context        // for input.ValidateUserInput
	inputsValues map[string]interface{} // for input.Available
}

// AskUseTemplateOptions - dialog for using the template in the project.
func (p *Dialogs) AskUseTemplateOptions(projectState *project.State, inputs template.StepsGroups, opts *options.Options) (useTemplate.Options, error) {
	dialog := &useTmplDialog{
		Dialogs:      p,
		projectState: projectState,
		options:      opts,
		context:      context.Background(),
		inputsValues: make(map[string]interface{}),
	}
	return dialog.ask(inputs)
}

func (d *useTmplDialog) ask(inputs input.StepsGroups) (useTemplate.Options, error) {
	// Load inputs file
	if d.options.IsSet(inputsFileFlag) {
		path := d.options.GetString(inputsFileFlag)
		content, err := os.ReadFile(path) // nolint:forbidigo // file may be outside the project, so the OS package is used
		if err != nil {
			return d.out, fmt.Errorf(`cannot read inputs file "%s": %w`, path, err)
		}
		if err := json.Decode(content, &d.inputsFile); err != nil {
			return d.out, fmt.Errorf(`cannot decode inputs file "%s": %w`, path, err)
		}
	}

	// Target branch
	targetBranch, err := d.SelectBranch(d.options, d.projectState.LocalObjects().Branches(), `Select the target branch`)
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

// addInputValue from CLI dialog or inputs file.
func (d *useTmplDialog) addInputValue(value interface{}, inputDef input.Input, validate bool) error {
	// Convert
	value, err := inputDef.Type.ParseValue(value)
	if err != nil {
		return fmt.Errorf("invalid template input: %w", err)
	}

	// Validate
	if validate {
		if err := inputDef.ValidateUserInput(value, d.context); err != nil {
			return fmt.Errorf("invalid template input: %w", err)
		}
	}

	// Add
	d.context = context.WithValue(d.context, contextKey(inputDef.Id), value)
	d.inputsValues[inputDef.Id] = value
	d.out.Inputs = append(d.out.Inputs, template.InputValue{Id: inputDef.Id, Value: value})
	return nil
}

func (d *useTmplDialog) askInputs(inputs input.StepsGroups) error {
	for _, group := range inputs {
		stepsToShow, announceGroup := d.selectStepsToShow(group)
		for _, index := range stepsToShow {
			step := group.Steps[index]

			announceStep := true
			for _, inputDef := range step.Inputs {
				if result, err := inputDef.Available(d.inputsValues); err != nil {
					return err
				} else if !result {
					// Input is hidden, add default/empty value
					if err := d.addInputValue(inputDef.DefaultOrEmpty(), inputDef, false); err != nil {
						return err
					}
					continue
				}
				var groupToAnnounce *input.StepsGroup
				if announceGroup {
					groupToAnnounce = group
				}
				var stepToAnnounce *input.Step
				if announceStep {
					stepToAnnounce = step
				}
				if err := d.askInput(inputDef, groupToAnnounce, stepToAnnounce); err != nil {
					return err
				}
				announceGroup = false
				announceStep = false
			}
		}
	}
	return nil
}

func (d *useTmplDialog) selectStepsToShow(group *input.StepsGroup) ([]int, bool) {
	var stepsToShow []int
	announceGroup := true
	if group.ShowStepsSelect() {
		d.Printf("%s\n", group.Description)
		announceGroup = false
		multiSelect := &prompt.MultiSelectIndex{
			Label:   "Select steps",
			Options: group.Steps.SelectOptions(),
			Validator: func(answersRaw interface{}) error {
				answers := answersRaw.([]survey.OptionAnswer)
				values := make([]string, len(answers))
				for i, v := range answers {
					values[i] = v.Value
				}
				return group.ValidateSelectedSteps(len(values))
			},
		}
		// Selected steps
		stepsToShow, _ = d.MultiSelectIndex(multiSelect)
	} else {
		// All steps
		stepsToShow = make([]int, len(group.Steps))
		for i := range stepsToShow {
			stepsToShow[i] = i
		}
	}
	return stepsToShow, announceGroup
}

func (d *useTmplDialog) askInput(inputDef input.Input, groupToAnnounce *input.StepsGroup, stepToAnnounce *input.Step) error {
	// Use value from the inputs file, if it is present
	if v, found := d.inputsFile[inputDef.Id]; found {
		// Validate and save
		return d.addInputValue(v, inputDef, true)
	}

	if groupToAnnounce != nil {
		d.Printf("%s\n", groupToAnnounce.Description)
	}

	if stepToAnnounce != nil {
		d.Printf("%s\n%s", stepToAnnounce.NameFoDialog(), stepToAnnounce.DescriptionForDialog())
	}

	// Ask for input
	switch inputDef.Kind {
	case input.KindInput, input.KindHidden, input.KindTextarea:
		question := &prompt.Question{
			Label:       inputDef.Name,
			Description: inputDef.Description,
			Validator: func(raw interface{}) error {
				value, err := inputDef.Type.ParseValue(raw)
				if err != nil {
					return err
				}
				return inputDef.ValidateUserInput(value, d.context)
			},
			Default: cast.ToString(inputDef.Default),
			Hidden:  inputDef.Kind == input.KindHidden,
		}

		var value string
		if inputDef.Kind == input.KindTextarea {
			value, _ = d.Editor("txt", question)
		} else {
			value, _ = d.Ask(question)
		}

		// Save value
		if err := d.addInputValue(value, inputDef, true); err != nil {
			return err
		}
	case input.KindConfirm:
		confirm := &prompt.Confirm{
			Label:       inputDef.Name,
			Description: inputDef.Description,
		}
		confirm.Default, _ = inputDef.Default.(bool)
		return d.addInputValue(d.Confirm(confirm), inputDef, true)
	case input.KindSelect:
		selectPrompt := &prompt.SelectIndex{
			Label:       inputDef.Name,
			Description: inputDef.Description,
			Options:     inputDef.Options.Names(),
			UseDefault:  true,
			Validator: func(answerRaw interface{}) error {
				answer := answerRaw.(survey.OptionAnswer)
				return inputDef.ValidateUserInput(answer.Value, d.context)
			},
		}
		if inputDef.Default != nil {
			if _, index, found := inputDef.Options.GetById(inputDef.Default.(string)); found {
				selectPrompt.Default = index
			}
		}
		selectedIndex, _ := d.SelectIndex(selectPrompt)
		return d.addInputValue(inputDef.Options[selectedIndex].Id, inputDef, true)
	case input.KindMultiSelect:
		multiSelect := &prompt.MultiSelectIndex{
			Label:       inputDef.Name,
			Description: inputDef.Description,
			Options:     inputDef.Options.Names(),
			Validator: func(answersRaw interface{}) error {
				answers := answersRaw.([]survey.OptionAnswer)
				values := make([]string, len(answers))
				for i, v := range answers {
					values[i] = v.Value
				}
				return inputDef.ValidateUserInput(values, d.context)
			},
		}
		// Default indices
		if inputDef.Default != nil {
			defaultIndices := make([]int, 0)
			for _, id := range inputDef.Default.([]interface{}) {
				if _, index, found := inputDef.Options.GetById(id.(string)); found {
					defaultIndices = append(defaultIndices, index)
				}
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
		return d.addInputValue(selectedValues, inputDef, true)
	}

	return nil
}
