package dialog

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"

	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// stepsDialog to define steps and steps groups.
type stepsDialog struct {
	prompt      prompt.Prompt
	stepsGroups input.StepsGroups
}

func newStepsDialog(prompt prompt.Prompt) *stepsDialog {
	return &stepsDialog{prompt: prompt, stepsGroups: make(input.StepsGroups, 0)}
}

func (d *stepsDialog) ask() (input.StepsGroups, error) {
	result, _ := d.prompt.Editor("md", &prompt.Question{
		Description: `Please define steps and groups for user inputs specification.`,
		Default:     d.defaultValue(),
		Validator: func(val interface{}) error {
			if err := d.parse(val.(string)); err != nil {
				// Print errors to new line
				return utils.PrefixError("\n", err)
			}
			return nil
		},
	})
	return d.stepsGroups, d.parse(result)
}

func (d *stepsDialog) parse(result string) error {
	result = strhelper.StripHtmlComments(result)
	scanner := bufio.NewScanner(strings.NewReader(result))
	errors := utils.NewMultiError()
	lineNum := 0

	var currentGroup *input.StepsGroup
	var currentStep *input.Step

	var invalidDefinition bool

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
			// Group definition
			currentGroup = &input.StepsGroup{Steps: make([]*input.Step, 0)}
			currentStep = nil
			d.stepsGroups = append(d.stepsGroups, currentGroup)

			invalidDefinition = false
		case strings.HasPrefix(line, `### Step`):
			// Step definition
			m := regexpcache.MustCompile(`"([^"]+)"`).FindStringSubmatch(line)
			if m == nil {
				errors.Append(fmt.Errorf(`line %d: cannot parse group "%s"`, lineNum, line))
				invalidDefinition = true
				continue
			}
			if currentGroup == nil {
				errors.Append(fmt.Errorf(`line %d: there is no group for step "%s"`, lineNum, m[1]))
				invalidDefinition = true
				continue
			}
			currentStep = &input.Step{Id: m[1]}
			currentGroup.Steps = append(currentGroup.Steps, currentStep)

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
				errors.Append(fmt.Errorf(`line %d: required is not valid option for a step`, lineNum))
				continue
			}

			currentGroup.Required = in
		case strings.HasPrefix(line, `icon:`):
			if currentStep == nil {
				errors.Append(fmt.Errorf(`line %d: icon is not valid option`, lineNum))
				continue
			}
			currentStep.Icon = strings.TrimSpace(strings.TrimPrefix(line, `icon:`))
		case strings.HasPrefix(line, `name:`):
			if currentStep == nil {
				errors.Append(fmt.Errorf(`line %d: name is not valid option`, lineNum))
				continue
			}
			currentStep.Name = strings.TrimSpace(strings.TrimPrefix(line, `name:`))
		case strings.HasPrefix(line, `dialogName:`):
			if currentStep == nil {
				errors.Append(fmt.Errorf(`line %d: dialogName is not valid option`, lineNum))
				continue
			}
			currentStep.DialogName = strings.TrimSpace(strings.TrimPrefix(line, `dialogName:`))
		case strings.HasPrefix(line, `dialogDescription:`):
			if currentStep == nil {
				errors.Append(fmt.Errorf(`line %d: dialogDescription is not valid option`, lineNum))
				continue
			}
			currentStep.DialogDescription = strings.TrimSpace(strings.TrimPrefix(line, `dialogDescription:`))
		default:
			// Expected object definition
			errors.Append(fmt.Errorf(`line %d: cannot parse "%s"`, lineNum, strhelper.Truncate(line, 10, "...")))
			continue
		}
	}

	// Validate
	if len(d.stepsGroups) == 0 {
		return fmt.Errorf("input must contain at least 1 group")
	}
	if e := d.stepsGroups.Validate(); e != nil {
		// nolint: errorlint
		err := e.(*utils.MultiError)
		for index, item := range err.Errors {
			msg := err.Error()

			// Replace step and group by index. Example:
			//   before: [0].steps[0].default
			//   after:  group 1, step "my-step": default
			regex := regexpcache.MustCompile(`^\[(\d+)\].steps\[(\d+)\].`)
			submatch := regex.FindStringSubmatch(item.Error())
			if submatch != nil {
				msg = regex.ReplaceAllStringFunc(item.Error(), func(s string) string {
					groupIndex, _ := strconv.Atoi(submatch[1])
					stepIndex, _ := strconv.Atoi(submatch[2])
					groupOrder := groupIndex + 1
					return fmt.Sprintf(`group %d, step "%s": `, groupOrder, d.stepsGroups[groupIndex].Steps[stepIndex].Id)
				})
			} else {
				// Replace group by index. Example:
				//   before: [0].default
				//   after:  group 1: default
				regex = regexpcache.MustCompile(`^\[(\d+)\].`)
				submatch = regex.FindStringSubmatch(item.Error())
				if submatch != nil {
					msg = regex.ReplaceAllStringFunc(item.Error(), func(s string) string {
						groupIndex, _ := strconv.Atoi(submatch[1])
						groupOrder := groupIndex + 1
						return fmt.Sprintf(`group %d: `, groupOrder)
					})
				}
			}

			msg = strings.Replace(msg, "steps must contain at least 1 item", "steps must contain at least 1 step", 1)

			err.Errors[index] = fmt.Errorf(msg)
		}
		errors.Append(err)
	}

	return errors.ErrorOrNil()
}

func (d *stepsDialog) defaultValue() string {
	// File header - info for user
	return `
<!--
Please create steps and groups for the user inputs.
There is one group and one step predefined. Feel free to change them and/or create others.

required option for group specifies how many steps need to be filled by user of the template
	allowed values: all, atLeastOne, exactOne, zeroOrOne, optional
-->

## Group
description: Default Group
required: all

### Step "step-1"
icon: common
name: Default Step
description: Default Step

`
}
