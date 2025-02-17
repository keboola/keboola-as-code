package dialog

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	markdown "github.com/MichaelMure/go-term-markdown"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/cast"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type useTmplInputsDialog struct {
	*Dialogs
	groups        input.StepsGroupsExt
	inputs        map[string]*input.Input
	inputsFile    map[string]any // inputs values loaded from a file specified by inputsFileFlag
	useInputsFile bool
	out           template.InputsValues
	context       context.Context // for input.ValidateUserInput
	inputsValues  map[string]any  // for input.Available
}

// AskUseTemplateInputs - dialog to enter template inputs.
func (p *Dialogs) AskUseTemplateInputs(ctx context.Context, groups input.StepsGroupsExt, isForTest bool, inputsFileFlag configmap.Value[string]) (template.InputsValues, []string, error) {
	dialog := &useTmplInputsDialog{
		Dialogs:      p,
		groups:       groups,
		inputs:       groups.InputsMap(),
		context:      context.WithoutCancel(ctx),
		inputsValues: make(map[string]any),
	}
	return dialog.ask(ctx, isForTest, inputsFileFlag)
}

func (d *useTmplInputsDialog) ask(ctx context.Context, isForTest bool, inputsFile configmap.Value[string]) (template.InputsValues, []string, error) {
	// Load inputs file
	if inputsFile.IsSet() {
		d.useInputsFile = true
		path := inputsFile.Value
		content, err := os.ReadFile(path) // nolint:forbidigo // file may be outside the project, so the OS package is used
		if err != nil {
			return d.out, nil, errors.Errorf(`cannot read inputs file "%s": %w`, path, err)
		}
		if err := json.Decode(content, &d.inputsFile); err != nil {
			return d.out, nil, errors.Errorf(`cannot decode inputs file "%s": %w`, path, err)
		}
	}

	warnings := make([]string, 0)
	err := d.groups.VisitInputs(func(group *input.StepsGroupExt, step *input.StepExt, inputDef *input.Input) error {
		// Print info about group and select steps
		if !group.Announced {
			if err := d.announceGroup(group, isForTest); err != nil {
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

		// Use empty value if we don't ask for the input
		if !available {
			return d.addInputValue(ctx, inputDef.Empty(), inputDef, false)
		}

		// Print info about step
		if step.Show && !step.Announced {
			if err := d.announceStep(step); err != nil {
				return err
			}
		}

		// Use value from the inputs file, if it is present
		if d.useInputsFile {
			if v, found := d.inputsFile[inputDef.ID]; found {
				if err := d.addInputValue(ctx, v, inputDef, true); err != nil {
					return errors.NewNestedError(err, errors.New("please fix the value in the inputs JSON file"))
				}
			} else {
				if err := d.addInputValue(ctx, d.defaultOrEmptyValueFor(inputDef), inputDef, true); err != nil {
					return errors.NewNestedError(err, errors.New("please define value in the inputs JSON file"))
				}
			}
			return nil
		}

		// Ask for the input
		warning, err := d.askInput(ctx, inputDef, isForTest)
		if err != nil {
			return err
		}
		if warning != "" {
			warnings = append(warnings, warning)
		}
		return nil
	})

	return d.out, warnings, err
}

func (d *useTmplInputsDialog) announceGroup(group *input.StepsGroupExt, isForTest bool) error {
	// Only once
	if group.Announced {
		return nil
	}
	group.Announced = true

	// Print description
	d.Printf("%s\n", group.Description)

	// Determine selected steps
	var selectedSteps []int
	switch {
	case d.useInputsFile:
		// Detect steps from the inputs file, if present.
		// If at least one input value is found, then the step is marked as selected.
		for stepIndex, step := range group.Steps {
			// Is step pre-selected (on upgrade operation)
			if step.Show {
				selectedSteps = append(selectedSteps, stepIndex)
				continue
			}

			// Is at least one input defined in the inputs file?
			for _, inputDef := range step.Inputs {
				if _, found := d.inputsFile[inputDef.ID]; found {
					selectedSteps = append(selectedSteps, stepIndex)
					break // check next step
				}
			}
		}
	case !group.AreStepsSelectable() || isForTest:
		// Are all steps required? -> skip select box
		// Want to test all cases for template test
		for stepIndex := range group.Steps {
			selectedSteps = append(selectedSteps, stepIndex)
		}
	default:
		// Prepare select box
		multiSelect := &prompt.MultiSelectIndex{
			Label:   "Select steps",
			Options: group.Steps.OptionsForSelectBox(),
			Default: group.Steps.SelectedOptions(),
			Validator: func(answersRaw any) error {
				answers := answersRaw.([]survey.OptionAnswer)
				values := make([]string, len(answers))
				for i, v := range answers {
					values[i] = v.Value
				}
				return group.ValidateStepsCount(len(group.Steps), len(values))
			},
		}

		// Show select box
		selectedSteps, _ = d.MultiSelectIndex(multiSelect)
	}

	// Validate steps count
	if err := group.ValidateStepsCount(len(group.Steps), len(selectedSteps)); err != nil && !isForTest {
		details := errors.NewMultiError()
		details.Append(err)
		details.Append(errors.Errorf("number of selected steps (%d) is incorrect", len(selectedSteps)))
		if d.useInputsFile {
			// List found inputs
			foundInputs := orderedmap.New()
			for _, step := range group.Steps {
				for _, inputDef := range step.Inputs {
					if _, found := d.inputsFile[inputDef.ID]; found {
						v, _ := foundInputs.GetOrNil(step.Name).([]string)
						foundInputs.Set(step.Name, append(v, inputDef.ID))
					}
				}
			}

			// Convert list to error message
			if foundInputs.Len() > 0 {
				foundInputsErr := errors.NewMultiError()
				for _, step := range foundInputs.Keys() {
					inputs := foundInputs.GetOrNil(step).([]string)
					foundInputsErr.Append(errors.Errorf(`%s, inputs: %s`, step, strings.Join(inputs, ", ")))
				}
				details.AppendWithPrefix(foundInputsErr, "in the inputs JSON file, these steps are defined")
			} else {
				details.Append(errors.New("there are no inputs for this group in the inputs JSON file"))
			}
		}
		return errors.NewNestedError(
			errors.Errorf(`steps group %d "%s" is invalid`, group.GroupIndex+1, group.Description),
			details,
		)
	}

	// Mark selected steps
	for _, selectedIndex := range selectedSteps {
		group.Steps[selectedIndex].Show = true
	}
	return nil
}

func (d *useTmplInputsDialog) announceStep(step *input.StepExt) error {
	// Only once
	if step.Announced {
		return nil
	}
	step.Announced = true

	// Print description
	d.Printf("%s\n%s", step.NameForDialog(), markdown.Render(step.DescriptionForDialog(), 80, 0))
	return nil
}

func (d *useTmplInputsDialog) askInput(ctx context.Context, inputDef *input.Input, isForTest bool) (string, error) {
	// Ask for input

	if inputDef.Kind == input.KindHidden && isForTest {
		// Put placeholders for env vars to tests instead of the values
		question := &prompt.Question{
			Label:       inputDef.Name,
			Description: fmt.Sprintf(`Enter the name of the environment variable that will fill input "%s". Note that it will get prefix KBC_SECRET_.`, inputDef.Name),
			Validator: func(raw any) error {
				value, err := inputDef.Type.ParseValue(raw)
				if err != nil {
					return err
				}

				strValue := strings.ToUpper(cast.ToString(value))
				if !regexpcache.MustCompile(`^[A-Z0-9\_]+$`).MatchString(strValue) {
					return errors.Errorf(`the variable name "%s" is invalid, it can contain only uppercase letters, numbers and underscores`, strValue)
				}
				if strings.HasSuffix(strValue, "KBC_SECRET_") {
					return errors.New(`do not start the variable name with KBC_SECRET_ prefix, it will be added automatically`)
				}
				return nil
			},
			Default: cast.ToString(inputDef.Default),
			Hidden:  false,
		}

		value, _ := d.Ask(question)
		envVar := strings.ToUpper(fmt.Sprintf("KBC_SECRET_%s", value))
		// Add the env var to the input as placeholder, that's the reason for the surrounding '##'
		err := d.addInputValue(ctx, fmt.Sprintf("##%s##", envVar), inputDef, true)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf(`Input "%s" expects setting of environment variable "%s".`, inputDef.Name, envVar), nil
	}

	switch inputDef.Kind {
	case input.KindInput, input.KindHidden, input.KindTextarea:
		question := &prompt.Question{
			Label:       inputDef.Name,
			Description: inputDef.Description,
			Validator: func(raw any) error {
				value, err := inputDef.Type.ParseValue(raw)
				if err != nil {
					return err
				}
				return inputDef.ValidateUserInput(ctx, value)
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

		return "", d.addInputValue(ctx, value, inputDef, true)
	case input.KindConfirm:
		confirm := &prompt.Confirm{
			Label:       inputDef.Name,
			Description: inputDef.Description,
		}
		confirm.Default, _ = inputDef.Default.(bool)
		return "", d.addInputValue(ctx, d.Confirm(confirm), inputDef, true)
	case input.KindSelect:
		selectPrompt := &prompt.SelectIndex{
			Label:       inputDef.Name,
			Description: inputDef.Description,
			Options:     inputDef.Options.Names(),
			UseDefault:  true,
			Validator: func(answerRaw any) error {
				return inputDef.ValidateUserInput(ctx, answerRaw.(survey.OptionAnswer).Value)
			},
		}
		if inputDef.Default != nil {
			if _, index, found := inputDef.Options.GetByID(inputDef.Default.(string)); found {
				selectPrompt.Default = index
			}
		}
		selectedIndex, _ := d.SelectIndex(selectPrompt)
		return "", d.addInputValue(ctx, inputDef.Options[selectedIndex].Value, inputDef, true)
	case input.KindMultiSelect:
		multiSelect := &prompt.MultiSelectIndex{
			Label:       inputDef.Name,
			Description: inputDef.Description,
			Options:     inputDef.Options.Names(),
			Validator: func(answersRaw any) error {
				answers := answersRaw.([]survey.OptionAnswer)
				values := make([]string, len(answers))
				for i, v := range answers {
					values[i] = v.Value
				}
				return inputDef.ValidateUserInput(ctx, values)
			},
		}
		// Default indices
		if inputDef.Default != nil {
			defaultIndices := make([]int, 0)
			for _, id := range inputDef.Default.([]any) {
				if _, index, found := inputDef.Options.GetByID(id.(string)); found {
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
		return "", d.addInputValue(ctx, selectedValues, inputDef, true)
	case input.KindOAuth:
		// OAuth is not supported in CLI dialog.
		return "", d.addInputValue(ctx, d.defaultOrEmptyValueFor(inputDef), inputDef, true)
	case input.KindOAuthAccounts:
		// OAuth is not supported in CLI dialog.
		return "", d.addInputValue(ctx, d.defaultOrEmptyValueFor(inputDef), inputDef, true)
	}

	return "", nil
}

// addInputValue from CLI dialog or inputs file.
func (d *useTmplInputsDialog) addInputValue(ctx context.Context, value any, inputDef *input.Input, isFilled bool) error {
	inputValue, err := template.ParseInputValue(ctx, value, inputDef, isFilled)
	if err != nil {
		return err
	}

	d.inputsValues[inputDef.ID] = inputValue.Value
	d.out = append(d.out, inputValue)
	return nil
}

func (d *useTmplInputsDialog) defaultOrEmptyValueFor(inputDef *input.Input) any {
	switch inputDef.Kind {
	case input.KindOAuthAccounts:
		// OAuth is not supported in CLI dialog.
		value := inputDef.DefaultOrEmpty()
		if inputDef.Default == nil {
			// Get component ID
			oauthInput, found := d.inputs[inputDef.OauthInputID]
			if !found {
				panic(errors.Errorf(`oauth input "%s" not found`, inputDef.OauthInputID))
			}
			if oauthInput.Kind != input.KindOAuth {
				panic(errors.Errorf(`input "%s" has unexpected kind, expected "%s", given "%s"`, inputDef.OauthInputID, input.KindOAuth, oauthInput.Kind))
			}
			componentID := oauthInput.ComponentID

			// User must fill in value in UI,
			// but at least empty keys must be generated in CLI,
			// so values can be found during the upgrade operation.
			if v, found := input.OauthAccountsEmptyValue[componentID]; found {
				value = v
			}
		}
		return value
	default:
		return inputDef.DefaultOrEmpty()
	}
}
