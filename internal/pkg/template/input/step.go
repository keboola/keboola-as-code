package input

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cast"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func Load(fs filesystem.Fs) (StepsGroups, error) {
	f, err := loadFile(fs)
	if err != nil {
		return nil, err
	}
	return f.StepsGroups, nil
}

type StepsGroups []StepsGroup

// Save inputs to the FileName.
func (g StepsGroups) Save(fs filesystem.Fs) error {
	if err := saveFile(fs, &file{StepsGroups: g}); err != nil {
		return err
	}
	return nil
}

func (g StepsGroups) Path() string {
	return Path()
}

func (g StepsGroups) Validate() error {
	errors := utils.NewMultiError()

	if len(g) == 0 {
		errors.Append(fmt.Errorf("at least one steps group must be defined"))
	}

	// Check duplicate inputs
	inputsOccurrences := orderedmap.New()
	_ = g.ToExtended().VisitInputs(func(group *StepsGroupExt, step *StepExt, input *Input) error {
		vRaw, _ := inputsOccurrences.Get(input.Id)
		v, _ := vRaw.([]string)
		v = append(v, fmt.Sprintf(`group %d, step %d "%s"`, step.GroupIndex+1, step.StepIndex+1, step.Name))
		inputsOccurrences.Set(input.Id, v)
		return nil
	})
	inputsOccurrences.SortKeys(func(keys []string) {
		sort.Strings(keys)
	})
	for _, inputId := range inputsOccurrences.Keys() {
		occurrencesRaw, _ := inputsOccurrences.Get(inputId)
		occurrences := occurrencesRaw.([]string)
		if len(occurrences) > 1 {
			inputsErr := utils.NewMultiError()
			for _, occurrence := range occurrences {
				inputsErr.Append(fmt.Errorf(occurrence))
			}
			errors.AppendWithPrefix(fmt.Sprintf(`input "%s" is defined %d times in`, inputId, len(occurrences)), inputsErr)
		}
	}

	// Validate other rules
	if err := validate(g); err != nil {
		errors.Append(err)
	}

	// Enhance error messages
	for index, item := range errors.Errors {
		msg := item.Error()

		// Replace step and group by index. Example:
		//   before: [0].steps[0].inputs[0].default
		//   after:  group 1, step 1, input "foo.bar": default
		regex := regexpcache.MustCompile(`^\[(\d+)\](?:\.steps\[(\d+)\])?(?:\.inputs\[(\d+)\])?\.`)
		submatch := regex.FindStringSubmatch(msg)
		if submatch == nil {
			continue
		}

		msg = regex.ReplaceAllStringFunc(msg, func(s string) string {
			var out strings.Builder

			// Group index
			groupIndex := cast.ToInt(submatch[1])
			out.WriteString("group ")
			out.WriteString(cast.ToString(groupIndex + 1))

			// Step index
			var stepIndex int
			if submatch[2] != "" {
				stepIndex = cast.ToInt(submatch[2])
				out.WriteString(", step ")
				out.WriteString(cast.ToString(stepIndex + 1))
			}

			// Input ID
			if submatch[3] != "" {
				inputIndex := cast.ToInt(strings.Trim(submatch[3], "[]."))
				out.WriteString(`, input "`)
				out.WriteString(g[groupIndex].Steps[stepIndex].Inputs.GetIndex(inputIndex).Id)
				out.WriteString(`"`)
			}

			out.WriteString(": ")
			return out.String()
		})

		msg = strings.Replace(msg, "steps must contain at least 1 item", "steps must contain at least 1 step", 1)
		errors.Errors[index] = fmt.Errorf(msg)
	}

	return errors.ErrorOrNil()
}

// StepsGroup is a container for Steps.
type StepsGroup struct {
	Description string         `json:"description" validate:"min=1,max=80"`
	Required    StepsCountRule `json:"required" validate:"oneof=all atLeastOne exactlyOne zeroOrOne optional"`
	Steps       Steps          `json:"steps" validate:"min=1,dive"`
}

type StepsCountRule string

const (
	RequiredAll                   = StepsCountRule("all")
	RequiredOptional              = StepsCountRule("optional")
	RequiredAtLeastOne            = StepsCountRule("atLeastOne")
	RequiredExactlyOne            = StepsCountRule("exactlyOne")
	RequiredZeroOrOne             = StepsCountRule("zeroOrOne")
	requiredAllDescription        = "all steps (%d) must be selected"
	requiredAtLeastOneDescription = "at least one step must be selected"
	requiredExactlyOneDescription = "exactly one step must be selected"
	requiredZeroOrOneDescription  = "zero or one step must be selected"
)

func (g StepsGroup) AreStepsSelectable() bool {
	return g.Required != RequiredAll &&
		(len(g.Steps) > 1 || (g.Required != RequiredAtLeastOne && g.Required != RequiredExactlyOne))
}

func (g StepsGroup) ValidateStepsCount(all, selected int) error {
	if g.Required == RequiredAll && selected < all {
		return fmt.Errorf(requiredAllDescription, all)
	}
	if g.Required == RequiredAtLeastOne && selected < 1 {
		return fmt.Errorf(requiredAtLeastOneDescription)
	}
	if g.Required == RequiredExactlyOne && selected != 1 {
		return fmt.Errorf(requiredExactlyOneDescription)
	}
	if g.Required == RequiredZeroOrOne && selected > 1 {
		return fmt.Errorf(requiredZeroOrOneDescription)
	}
	return nil
}

type Steps []Step

// Step is a container for Inputs.
type Step struct {
	Icon              string `json:"icon" validate:"required,templateicon,min=1"`
	Name              string `json:"name" validate:"required,min=1,max=25"`
	Description       string `json:"description" validate:"min=1,max=60"`
	DialogName        string `json:"dialogName,omitempty" validate:"omitempty,max=25"`
	DialogDescription string `json:"dialogDescription,omitempty" validate:"omitempty,max=200"`
	Inputs            Inputs `json:"inputs" validate:"omitempty,dive"`
}

func (s Step) NameFoDialog() string {
	if s.DialogName != "" {
		return s.DialogName
	}
	return s.Name
}

func (s Step) DescriptionForDialog() string {
	if s.DialogDescription != "" {
		return s.DialogDescription
	}
	return s.Description
}
