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

func (d *useTmplDialog) ask(stepsGroups input.StepsGroups) (useTemplate.Options, error) {
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
	if err := d.askInputs(stepsGroups.ToExtended()); err != nil {
		return d.out, err
	}

	return d.out, nil
}

func (d *useTmplDialog) askInputs(stepsGroups input.StepsGroupsExt) error {
	return stepsGroups.VisitInputs(func(group *input.StepsGroupExt, step *input.StepExt, inputDef *input.Input) error {
		// Print info about group and select steps
		if !group.Announced {
			if err := d.announceGroup(group); err != nil {
				return err
			}
		}

		// Determine if we should ask for the input
		available := false
		if step.Show {
			if v, err := inputDef.Available(d.inputsValues); err != nil {
				return err
			} else {
				available = v
			}
		}

		// Use default or empty value if we don't ask for the input
		if !available {
			return d.addInputValue(inputDef.DefaultOrEmpty(), inputDef, false)
		}

		// Print info about step
		if step.Show && !step.Announced {
			if err := d.announceStep(step); err != nil {
				return err
			}
		}

		// Ask for the input
		return d.askInput(inputDef)
	})
}

func (d *useTmplDialog) announceGroup(group *input.StepsGroupExt) error {
	// Only once
	if group.Announced {
		return nil
	}
	group.Announced = true

	// Print description
	d.Printf("%s\n", group.Description)

	// Are all steps required?
	if !group.AreStepsSelectable() {
		for _, step := range group.Steps {
			step.Show = true
		}
		return nil
	}

	// Prepare select box
	multiSelect := &prompt.MultiSelectIndex{
		Label:   "Select steps",
		Options: group.Steps.OptionsForSelectBox(),
		Validator: func(answersRaw interface{}) error {
			answers := answersRaw.([]survey.OptionAnswer)
			values := make([]string, len(answers))
			for i, v := range answers {
				values[i] = v.Value
			}
			return group.ValidateStepsCount(len(values))
		},
	}

	// Select steps
	selectedSteps, _ := d.MultiSelectIndex(multiSelect)
	if err := group.ValidateStepsCount(len(selectedSteps)); err != nil {
		return err
	}

	// Mark selected steps
	for _, selectedIndex := range selectedSteps {
		group.Steps[selectedIndex].Show = true
	}
	return nil
}

func (d *useTmplDialog) announceStep(step *input.StepExt) error {
	// Only once
	if step.Announced {
		return nil
	}
	step.Announced = true

	// Print description
	d.Printf("%s\n%s", step.NameFoDialog(), step.DescriptionForDialog())
	return nil
}

func (d *useTmplDialog) askInput(inputDef *input.Input) error {
	// Use value from the inputs file, if it is present
	if v, found := d.inputsFile[inputDef.Id]; found {
		// Validate and save
		return d.addInputValue(v, inputDef, true)
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
		return d.addInputValue(inputDef.Options[selectedIndex].Value, inputDef, true)
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
			selectedValues = append(selectedValues, inputDef.Options[selectedIndex].Value)
		}
		// Save value
		return d.addInputValue(selectedValues, inputDef, true)
	}

	return nil
}

// addInputValue from CLI dialog or inputs file.
func (d *useTmplDialog) addInputValue(value interface{}, inputDef *input.Input, isFiled bool) error {
	// Convert
	value, err := inputDef.Type.ParseValue(value)
	if err != nil {
		return fmt.Errorf("invalid template input: %w", err)
	}

	// Validate
	if isFiled {
		if err := inputDef.ValidateUserInput(value, d.context); err != nil {
			return fmt.Errorf("invalid template input: %w", err)
		}
	}

	// Add
	inputValue := template.InputValue{Id: inputDef.Id, Value: value, Skipped: !isFiled}
	d.context = context.WithValue(d.context, contextKey(inputDef.Id), value)
	d.inputsValues[inputDef.Id] = value
	d.out.Inputs = append(d.out.Inputs, inputValue)
	return nil
}
