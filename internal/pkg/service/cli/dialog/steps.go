package dialog

import (
	"bufio"
	"strings"

	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// stepsDialog to define steps and steps groups.
type stepsDialog struct {
	prompt prompt.Prompt
}

func newStepsDialog(prompt prompt.Prompt) *stepsDialog {
	return &stepsDialog{prompt: prompt}
}

func (d *stepsDialog) ask() (input.StepsGroupsExt, error) {
	result, _ := d.prompt.Editor("md", &prompt.Question{
		Description: `Please define steps and groups for user inputs specification.`,
		Default:     d.defaultValue(),
		Validator: func(val any) error {
			if _, err := d.parse(val.(string)); err != nil {
				// Print errors to new line
				return errors.PrefixError(err, "\n")
			}
			return nil
		},
	})
	return d.parse(result)
}

func (d *stepsDialog) parse(result string) (input.StepsGroupsExt, error) {
	result = strhelper.StripHTMLComments(result)
	scanner := bufio.NewScanner(strings.NewReader(result))
	errs := errors.NewMultiError()
	lineNum := 0

	var currentGroup *input.StepsGroupExt
	var currentStep *input.StepExt
	var invalidDefinition bool
	stepsGroups := make(input.StepsGroupsExt, 0)
	stepIds := make(map[string]bool)

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines
		if len(line) == 0 {
			continue
		}

		// Parse line
		switch {
		case strings.HasPrefix(line, `## Group`):
			// Create group
			currentGroup = &input.StepsGroupExt{GroupIndex: len(stepsGroups)}
			stepsGroups = append(stepsGroups, currentGroup)
			currentStep = nil
			invalidDefinition = false
		case strings.HasPrefix(line, `### Step`):
			// Step definition
			m := regexpcache.MustCompile(`"([^"]+)"`).FindStringSubmatch(line)
			if m == nil {
				errs.Append(errors.Errorf(`line %d: cannot parse group "%s"`, lineNum, line))
				invalidDefinition = true
				continue
			}

			if currentGroup == nil {
				errs.Append(errors.Errorf(`line %d: there needs to be a group definition before step "%s"`, lineNum, m[1]))
				invalidDefinition = true
				continue
			}

			// Step ID must be unique
			stepID := m[1]
			if stepIds[stepID] {
				errs.Append(errors.Errorf(`line %d: step with id "%s" is already defined`, lineNum, m[1]))
				invalidDefinition = true
				continue
			}
			stepIds[stepID] = true

			// Create step
			currentStep = &input.StepExt{
				GroupIndex: currentGroup.GroupIndex,
				StepIndex:  len(currentGroup.Steps),
				ID:         stepID,
			}
			currentGroup.AddStep(currentStep)
			invalidDefinition = false
		case invalidDefinition:
			// Skip lines after invalid definition
		case strings.HasPrefix(line, `description:`):
			in := strings.TrimSpace(strings.TrimPrefix(line, `description:`))
			if currentStep != nil {
				currentStep.Description = in
			} else {
				currentGroup.Description = in
			}
		case strings.HasPrefix(line, `required:`):
			in := strings.TrimSpace(strings.TrimPrefix(line, `required:`))
			if currentStep != nil {
				errs.Append(errors.Errorf(`line %d: required is not valid option for a step`, lineNum))
				continue
			}

			currentGroup.Required = input.StepsCountRule(in)
		case strings.HasPrefix(line, `icon:`):
			if currentStep == nil {
				errs.Append(errors.Errorf(`line %d: icon is not valid option`, lineNum))
				continue
			}
			currentStep.Icon = strings.TrimSpace(strings.TrimPrefix(line, `icon:`))
		case strings.HasPrefix(line, `name:`):
			if currentStep == nil {
				errs.Append(errors.Errorf(`line %d: name is not valid option`, lineNum))
				continue
			}
			currentStep.Name = strings.TrimSpace(strings.TrimPrefix(line, `name:`))
		case strings.HasPrefix(line, `dialogName:`):
			if currentStep == nil {
				errs.Append(errors.Errorf(`line %d: dialogName is not valid option`, lineNum))
				continue
			}
			currentStep.DialogName = strings.TrimSpace(strings.TrimPrefix(line, `dialogName:`))
		case strings.HasPrefix(line, `dialogDescription:`):
			if currentStep == nil {
				errs.Append(errors.Errorf(`line %d: dialogDescription is not valid option`, lineNum))
				continue
			}
			currentStep.DialogDescription = strings.TrimSpace(strings.TrimPrefix(line, `dialogDescription:`))
		default:
			// Expected object definition
			errs.Append(errors.Errorf(`line %d: cannot parse "%s"`, lineNum, strhelper.Truncate(line, 10, "...")))
			continue
		}
	}

	// Validate
	if err := stepsGroups.ValidateDefinitions(); err != nil {
		errs.Append(err)
	}

	return stepsGroups, errs.ErrorOrNil()
}

func (d *stepsDialog) defaultValue() string {
	// File header - info for user
	return `
<!--
Please create steps and groups for the user inputs.
There is one group and one step predefined. Feel free to change them and/or create others.

"required" option for group specifies how many steps need to be filled by user of the template
	allowed values: all, atLeastOne, exactlyOne, zeroOrOne, optional
-->

## Group
description: Default Group
required: all

### Step "step-1"
icon: common:settings
name: Default Step
description: Default Step

`
}
