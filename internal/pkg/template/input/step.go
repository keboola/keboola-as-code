package input

import (
	"fmt"
	"sort"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/cast"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func Load(fs filesystem.Fs, ctx *jsonnet.Context) (StepsGroups, error) {
	f, err := loadFile(fs, ctx)
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

// InputsMap returns all inputs in a map indexed by their ids.
func (g StepsGroups) InputsMap() map[string]*Input {
	res := make(map[string]*Input)
	_ = g.ToExtended().VisitInputs(func(group *StepsGroupExt, step *StepExt, input *Input) error {
		res[input.Id] = input
		return nil
	})
	return res
}

func (g StepsGroups) ValidateDefinitions() error {
	errs := errors.NewMultiError()

	if len(g) == 0 {
		errs.Append(errors.New("at least one steps group must be defined"))
	}

	inputsMap := make(map[string]*Input)
	inputsOccurrences := orderedmap.New() // inputId -> []string{occurrence1, ...}
	inputsReferences := orderedmap.New()  // inputId -> []string{referencedFromInputId1, ...}
	_ = g.ToExtended().VisitInputs(func(group *StepsGroupExt, step *StepExt, input *Input) error {
		inputsMap[input.Id] = input

		// Collect inputs occurrences
		{
			v, _ := inputsOccurrences.GetOrNil(input.Id).([]string)
			v = append(v, fmt.Sprintf(`group %d, step %d "%s"`, step.GroupIndex+1, step.StepIndex+1, step.Name))
			inputsOccurrences.Set(input.Id, v)
		}

		// Collect inputs references
		if input.Kind == KindOAuthAccounts && len(input.OauthInputId) > 0 {
			v, _ := inputsReferences.GetOrNil(input.OauthInputId).([]string)
			v = append(v, input.Id)
			inputsReferences.Set(input.OauthInputId, v)
		}

		return nil
	})

	// Check duplicate inputs
	inputsOccurrences.SortKeys(func(keys []string) {
		sort.Strings(keys)
	})
	for _, inputId := range inputsOccurrences.Keys() {
		occurrences, _ := inputsOccurrences.GetOrNil(inputId).([]string)
		if len(occurrences) > 1 {
			inputsErr := errors.NewMultiError()
			for _, occurrence := range occurrences {
				inputsErr.Append(errors.New(occurrence))
			}
			errs.AppendWithPrefixf(inputsErr, `input "%s" is defined %d times in`, inputId, len(occurrences))
		}
	}

	// Check all referenced inputs exist
	inputsReferences.SortKeys(func(keys []string) {
		sort.Strings(keys)
	})
	for _, inputId := range inputsReferences.Keys() {
		if _, found := inputsOccurrences.Get(inputId); !found {
			// Referenced input is missing
			inputsErr := errors.NewMultiError()
			references, _ := inputsReferences.GetOrNil(inputId).([]string)
			for _, referencedFrom := range references {
				inputsErr.Append(errors.New(referencedFrom))
			}
			errs.AppendWithPrefixf(inputsErr, `input "%s" not found, referenced from`, inputId)
		}
	}

	// Check multi-input rules
	for _, input := range inputsMap {
		// Check that input kind=KindOAuthAccounts is defined for a supported component
		if input.Kind == KindOAuthAccounts {
			if oauthInput, found := inputsMap[input.OauthInputId]; found {
				if !OauthAccountsSupportedComponents[oauthInput.ComponentId] {
					errs.Append(errors.Errorf(`input "%s" (kind=%s) is defined for "%s" component, but it is not supported`, input.Id, input.Kind, oauthInput.ComponentId))
				}
			}
		}
	}

	// Validate other rules
	if err := validateDefinitions(g); err != nil {
		errs.Append(err)
	}

	// Enhance error messages
	enhancedErrors := errors.NewMultiError()
	for _, item := range errs.WrappedErrors() {
		msg := item.Error()

		// Replace step and group by index. Example:
		//   before: [0].steps[0].inputs[0].default
		//   after:  group 1, step 1, input "foo.bar": default
		regex := regexpcache.MustCompile(`^\[(\d+)\](?:\.steps\[(\d+)\])?(?:\.inputs\[(\d+)\])?\.`)
		submatch := regex.FindStringSubmatch(msg)
		if submatch == nil {
			enhancedErrors.Append(errors.New(msg))
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
		enhancedErrors.Append(errors.New(msg))
	}

	return enhancedErrors.ErrorOrNil()
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
		return errors.Errorf(requiredAllDescription, all)
	}
	if g.Required == RequiredAtLeastOne && selected < 1 {
		return errors.New(requiredAtLeastOneDescription)
	}
	if g.Required == RequiredExactlyOne && selected != 1 {
		return errors.New(requiredExactlyOneDescription)
	}
	if g.Required == RequiredZeroOrOne && selected > 1 {
		return errors.New(requiredZeroOrOneDescription)
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
	DialogDescription string `json:"dialogDescription,omitempty" validate:"omitempty,mdmax=200"`
	Inputs            Inputs `json:"inputs" validate:"omitempty,dive"`
}

func (s Step) NameForDialog() string {
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
