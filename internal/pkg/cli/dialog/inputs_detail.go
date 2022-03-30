package dialog

import (
	"bufio"
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cast"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// inputsDetailDialog to define name/description for each user input.
type inputsDetailDialog struct {
	prompt prompt.Prompt
	inputs inputsMap
}

func newInputsDetailsDialog(prompt prompt.Prompt, inputs inputsMap) *inputsDetailDialog {
	return &inputsDetailDialog{prompt: prompt, inputs: inputs}
}

func (d *inputsDetailDialog) ask(stepsGroups input.StepsGroups, stepsToIds map[input.StepIndex]string) (*orderedmap.OrderedMap, error) {
	result, _ := d.prompt.Editor("md", &prompt.Question{
		Description: `Please complete the user inputs specification.`,
		Default:     d.defaultValue(stepsGroups, stepsToIds),
		Validator: func(val interface{}) error {
			_, err := d.parse(val.(string))
			if err != nil {
				// Print errors to new line
				return utils.PrefixError("\n", err)
			}

			return nil
		},
	})
	return d.parse(result)
}

func (d *inputsDetailDialog) parse(result string) (*orderedmap.OrderedMap, error) {
	result = strhelper.StripHtmlComments(result)
	scanner := bufio.NewScanner(strings.NewReader(result))
	errors := utils.NewMultiError()
	lineNum := 0

	order := make(map[string]int)
	orderVal := 0

	var currentInput *template.Input
	var invalidDefinition bool
	inputsToStepsMap := orderedmap.New()

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
			// Input definition
			m := regexpcache.MustCompile(`"([^"]+)"`).FindStringSubmatch(line)
			if m == nil {
				errors.Append(fmt.Errorf(`line %d: cannot parse config "%s"`, lineNum, line))
				invalidDefinition = true
				continue
			}
			i, found := d.inputs.get(m[1])
			if !found {
				errors.Append(fmt.Errorf(`line %d: input "%s" not found`, lineNum, m[1]))
				invalidDefinition = true
				continue
			}
			currentInput = i
			invalidDefinition = false
			orderVal++
			order[currentInput.Id] = orderVal
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
				errors.Append(fmt.Errorf(`line %d: %w`, lineNum, err))
				continue
			}
		case strings.HasPrefix(line, `options:`):
			if currentInput.Kind == input.KindSelect || currentInput.Kind == input.KindMultiSelect {
				optionsStr := strings.TrimSpace(strings.TrimPrefix(line, `options:`))
				if v, err := input.OptionsFromString(optionsStr); err == nil {
					currentInput.Options = v
				} else {
					errors.Append(fmt.Errorf(`line %d: %w`, lineNum, err))
					continue
				}
			} else {
				errors.Append(fmt.Errorf(`line %d: options are not expected for kind "%s"`, lineNum, currentInput.Kind))
				continue
			}
		case strings.HasPrefix(line, `step:`):
			inputsToStepsMap.Set(currentInput.Id, strings.TrimSpace(strings.TrimPrefix(line, `step:`)))
		default:
			// Expected object definition
			errors.Append(fmt.Errorf(`line %d: cannot parse "%s"`, lineNum, strhelper.Truncate(line, 10, "...")))
			continue
		}
	}

	// Validate
	allInputs := d.inputs.all()
	if e := allInputs.Validate(); e != nil {
		// nolint: errorlint
		err := e.(*utils.MultiError)
		for index, item := range err.Errors {
			// Replace input index by input ID. Example:
			//   before: [123].default
			//   after:  input "my-input": default
			msg := regexpcache.
				MustCompile(`^\[(\d+)\].`).
				ReplaceAllStringFunc(item.Error(), func(s string) string {
					return fmt.Sprintf(`input "%s": `, allInputs.GetIndex(cast.ToInt(strings.Trim(s, "[]."))).Id)
				})
			err.Errors[index] = fmt.Errorf(msg)
		}
		errors.Append(err)
	}

	// Sort
	d.inputs.data.SortKeys(func(keys []string) {
		sort.SliceStable(keys, func(i, j int) bool {
			return order[keys[i]] < order[keys[j]]
		})
	})

	return inputsToStepsMap, errors.ErrorOrNil()
}

func (d *inputsDetailDialog) defaultValue(stepsGroups input.StepsGroups, stepsToIds map[input.StepIndex]string) string {
	// File header - info for user
	fileHeader := `
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

ShowIf example, see: https://github.com/Knetic/govaluate/blob/master/MANUAL.md
    showIf: [some-previous-input] == "value"

Options format:
     kind: select
     default: id1
     options: {"id1":"Option 1","id2":"Option 2","id3":"Option 3"}

     kind: multiselect
     default: id1, id3
     options: {"id1":"Option 1","id2":"Option 2","id3":"Option 3"}

Preview of steps and groups you created:
`
	var defaultStepId string
	for gIdx, group := range stepsGroups {
		fileHeader += fmt.Sprintf(`- Group %d
`, gIdx+1)
		for sIdx := range group.Steps {
			index := input.StepIndex{Step: sIdx, Group: gIdx}
			fileHeader += fmt.Sprintf(`  - Step "%s"
`, stepsToIds[index])
			if gIdx == 0 && sIdx == 0 {
				defaultStepId = stepsToIds[index]
			}
		}
	}
	fileHeader += `
-->


`

	// Add definitions
	var lines strings.Builder
	lines.WriteString(fileHeader)
	for _, inputId := range d.inputs.ids() {
		i, _ := d.inputs.get(inputId)
		lines.WriteString(fmt.Sprintf("## Input \"%s\" (%s)\n", i.Id, i.Type))
		lines.WriteString(fmt.Sprintf("name: %s\n", i.Name))
		lines.WriteString(fmt.Sprintf("description: %s\n", i.Description))
		lines.WriteString(fmt.Sprintf("kind: %s\n", i.Kind))
		lines.WriteString(fmt.Sprintf("rules: %s\n", i.Rules))
		lines.WriteString(fmt.Sprintf("showIf: %s\n", i.If))

		// Default
		if slice, ok := i.Default.([]interface{}); ok && i.Kind == input.KindMultiSelect {
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

		lines.WriteString(fmt.Sprintf("step: %s\n", defaultStepId))

		lines.WriteString("\n")
	}

	return lines.String()
}
