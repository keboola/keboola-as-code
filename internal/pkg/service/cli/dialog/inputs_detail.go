package dialog

import (
	"bufio"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/keboola/go-utils/pkg/deepcopy"
	"github.com/spf13/cast"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// inputsDetailDialog to define name/description for each user input.
type inputsDetailDialog struct {
	prompt      prompt.Prompt
	inputs      input.InputsMap
	stepsGroups input.StepsGroupsExt
}

func newInputsDetailsDialog(prompt prompt.Prompt, inputs input.InputsMap, stepsGroups input.StepsGroupsExt) *inputsDetailDialog {
	return &inputsDetailDialog{prompt: prompt, inputs: inputs, stepsGroups: stepsGroups}
}

func (d *inputsDetailDialog) ask(ctx context.Context) (input.StepsGroupsExt, error) {
	result, _ := d.prompt.Editor("md", &prompt.Question{
		Description: `Please complete the user inputs specification.`,
		Default:     d.defaultValue(),
		Validator: func(val any) error {
			_, err := d.parse(ctx, val.(string))
			if err != nil {
				// Print errors to new line
				return errors.PrefixError(err, "\n")
			}

			return nil
		},
	})
	return d.parse(ctx, result)
}

func (d *inputsDetailDialog) parse(ctx context.Context, result string) (input.StepsGroupsExt, error) {
	result = strhelper.StripHTMLComments(result)
	scanner := bufio.NewScanner(strings.NewReader(result))
	errs := errors.NewMultiError()
	lineNum := 0

	var currentInput *template.Input
	var inputStep *input.StepExt
	var invalidDefinition bool
	stepGroups := deepcopy.Copy(d.stepsGroups).(input.StepsGroupsExt) // clone, so original value is not modified
	stepsMap := stepGroups.StepsMap()
	inputsOrder := make(map[string]int)

	// Input finalization function
	finalizeInput := func() {
		if currentInput == nil || invalidDefinition {
			return
		}

		// Check that step is defined
		if inputStep == nil {
			errs.Append(errors.Errorf(`input "%s": "step" is not defined`, currentInput.ID))
			return
		}

		// Add input to the step
		inputStep.AddInput(*currentInput)
	}

	// Parse all lines
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines
		if len(line) == 0 {
			continue
		}

		// Parse line
		switch {
		case strings.HasPrefix(line, `## Input`):
			// Finalize previous input
			finalizeInput()

			// Input definition
			m := regexpcache.MustCompile(`"([^"]+)"`).FindStringSubmatch(line)
			if m == nil {
				errs.Append(errors.Errorf(`line %d: cannot parse config "%s"`, lineNum, line))
				invalidDefinition = true
				continue
			}
			inputID := m[1]
			i, found := d.inputs.Get(inputID)
			if !found {
				errs.Append(errors.Errorf(`line %d: input "%s" not found`, lineNum, inputID))
				invalidDefinition = true
				continue
			}
			currentInput = i
			inputStep = nil
			invalidDefinition = false
			inputsOrder[currentInput.ID] = len(inputsOrder)
		case invalidDefinition:
			// Skip lines after invalid definition
		case strings.HasPrefix(line, `name:`):
			currentInput.Name = strings.TrimSpace(strings.TrimPrefix(line, `name:`))
		case strings.HasPrefix(line, `description:`):
			currentInput.Description = strings.TrimSpace(strings.TrimPrefix(line, `description:`))
		case strings.HasPrefix(line, `kind:`):
			currentInput.Kind = input.Kind(strings.TrimSpace(strings.TrimPrefix(line, `kind:`)))
		case strings.HasPrefix(line, `rules:`):
			currentInput.Rules = input.Rules(strings.TrimSpace(strings.TrimPrefix(line, `rules:`)))
		case strings.HasPrefix(line, `showIf:`):
			currentInput.If = input.If(strings.TrimSpace(strings.TrimPrefix(line, `showIf:`)))
		case strings.HasPrefix(line, `default:`):
			defaultStr := strings.TrimSpace(strings.TrimPrefix(line, `default:`))
			if defaultStr == "" {
				currentInput.Default = nil
			} else if v, err := currentInput.Type.ParseValue(defaultStr); err == nil {
				currentInput.Default = v
			} else {
				errs.Append(errors.Errorf(`line %d: %w`, lineNum, err))
				continue
			}
		case strings.HasPrefix(line, `options:`):
			if currentInput.Kind == input.KindSelect || currentInput.Kind == input.KindMultiSelect {
				optionsStr := strings.TrimSpace(strings.TrimPrefix(line, `options:`))
				if v, err := input.OptionsFromString(optionsStr); err == nil {
					currentInput.Options = v
				} else {
					errs.Append(errors.Errorf(`line %d: %w`, lineNum, err))
					continue
				}
			} else {
				errs.Append(errors.Errorf(`line %d: options are not expected for kind "%s"`, lineNum, currentInput.Kind))
				continue
			}
		case strings.HasPrefix(line, `step:`):
			stepID := strings.TrimSpace(strings.TrimPrefix(line, `step:`))
			step, ok := stepsMap[stepID]
			if !ok {
				errs.Append(errors.Errorf(`line %d: step "%s" not found`, lineNum, stepID))
				invalidDefinition = true
				continue
			}
			inputStep = step
		default:
			// Expected object definition
			errs.Append(errors.Errorf(`line %d: cannot parse "%s"`, lineNum, strhelper.Truncate(line, 10, "...")))
			continue
		}
	}

	// Finalize last input
	finalizeInput()

	// Validate
	if err := d.inputs.All().ValidateDefinitions(ctx); err != nil {
		errs.Append(err)
	}

	// Sort
	d.inputs.Sort(func(inputsIds []string) {
		sort.SliceStable(inputsIds, func(i, j int) bool {
			return inputsOrder[inputsIds[i]] < inputsOrder[inputsIds[j]]
		})
	})

	return stepGroups, errs.ErrorOrNil()
}

func (d *inputsDetailDialog) defaultValue() string {
	// File header - info for user
	var fileHeader strings.Builder
	fileHeader.WriteString(`
<!--
Please complete definition of the user inputs.
Edit lines below "## Input ...".
Do not edit lines starting with "#"!

1. Adjust the name, description, ... for each user input.

2. Sort the user inputs - move text blocks. 
   User will be asked for inputs in the specified order.

Allowed combinations of input type and kind (visual style):
   string:        text
    input         one line text
    hidden        one line text, characters are masked
    textarea      multi-line text
    select        drop-down list, one option must be selected

   int:           whole number
    input         one line text

   double:        decimal number
    input         one line text

   bool:          true/false
    confirm       yes/no prompt

   string[]:      array of strings
    multiselect   drop-down list, multiple options can be selected

Rules example, see: https://github.com/go-playground/validator/blob/master/README.md
    rules: required,email

ShowIf example, see: https://expr-lang.org/docs/language-definition
    showIf: [some-previous-input] == "value"

Options format:
     kind: select
     default: value1
     options: {"value1":"Label 1","value2":"Label 2","value3":"Label 3"}

     kind: multiselect
     default: value1, value3
     options: {"value1":"Label 1","value2":"Label 2","value3":"Label 3"}

Preview of steps and groups you created:
`)
	var defaultStepID string
	for _, group := range d.stepsGroups {
		fileHeader.WriteString(fmt.Sprintf("- Group %d: %s\n", group.GroupIndex+1, group.Description))
		for _, step := range group.Steps {
			fileHeader.WriteString(fmt.Sprintf("  - Step \"%s\": %s - %s\n", step.ID, step.Name, step.Description))
			if step.GroupIndex == 0 && step.StepIndex == 0 {
				defaultStepID = step.ID
			}
		}
	}
	fileHeader.WriteString(`
-->


`)

	// Add definitions
	var lines strings.Builder
	lines.WriteString(fileHeader.String())
	for _, inputID := range d.inputs.Ids() {
		i, _ := d.inputs.Get(inputID)
		lines.WriteString(fmt.Sprintf("## Input \"%s\" (%s)\n", i.ID, i.Type))
		lines.WriteString(fmt.Sprintf("name: %s\n", i.Name))
		lines.WriteString(fmt.Sprintf("description: %s\n", i.Description))
		lines.WriteString(fmt.Sprintf("kind: %s\n", i.Kind))
		lines.WriteString(fmt.Sprintf("rules: %s\n", i.Rules))
		lines.WriteString(fmt.Sprintf("showIf: %s\n", i.If))

		// Default
		if slice, ok := i.Default.([]any); ok && i.Kind == input.KindMultiSelect {
			var items []string
			for _, item := range slice {
				items = append(items, item.(string))
			}
			lines.WriteString(fmt.Sprintf("default: %s\n", strings.Join(items, ", ")))
		} else {
			lines.WriteString(fmt.Sprintf("default: %s\n", cast.ToString(i.Default)))
		}

		// Options
		if i.Options != nil {
			lines.WriteString(fmt.Sprintf("options: %s\n", json.MustEncode(i.Options.Map(), false)))
		}

		lines.WriteString(fmt.Sprintf("step: %s\n", defaultStepID))

		lines.WriteString("\n")
	}

	return lines.String()
}
