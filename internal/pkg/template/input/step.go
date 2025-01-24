package input

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/cast"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func Load(ctx context.Context, fs filesystem.Fs, jsonnetCtx *jsonnet.Context) (StepsGroups, error) {
	f, err := loadFile(ctx, fs, jsonnetCtx)
	if err != nil {
		return nil, err
	}
	return f.StepsGroups, nil
}

type StepsGroups []StepsGroup

// Save inputs to the FileName.
func (g StepsGroups) Save(ctx context.Context, fs filesystem.Fs) error {
	if err := saveFile(ctx, fs, &file{StepsGroups: g}); err != nil {
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
		res[input.ID] = input
		return nil
	})
	return res
}

func (g StepsGroups) ValidateDefinitions(ctx context.Context) error {
	errs := errors.NewMultiError()

	if len(g) == 0 {
		errs.Append(errors.New("at least one steps group must be defined"))
	}

	inputsMap := make(map[string]*Input)
	inputsOccurrences := orderedmap.New() // inputId -> []string{occurrence1, ...}
	inputsReferences := orderedmap.New()  // inputId -> []string{referencedFromInputId1, ...}
	_ = g.ToExtended().VisitInputs(func(group *StepsGroupExt, step *StepExt, input *Input) error {
		inputsMap[input.ID] = input

		// Collect inputs occurrences
		{
			v, _ := inputsOccurrences.GetOrNil(input.ID).([]string)
			v = append(v, fmt.Sprintf(`group %d, step %d "%s"`, step.GroupIndex+1, step.StepIndex+1, step.Name))
			inputsOccurrences.Set(input.ID, v)
		}

		// Collect inputs references
		if input.Kind == KindOAuthAccounts && len(input.OauthInputID) > 0 {
			v, _ := inputsReferences.GetOrNil(input.OauthInputID).([]string)
			v = append(v, input.ID)
			inputsReferences.Set(input.OauthInputID, v)
		}

		return nil
	})

	// Check duplicate inputs
	inputsOccurrences.SortKeys(func(keys []string) {
		sort.Strings(keys)
	})
	for _, inputID := range inputsOccurrences.Keys() {
		occurrences, _ := inputsOccurrences.GetOrNil(inputID).([]string)
		if len(occurrences) > 1 {
			inputsErr := errors.NewMultiError()
			for _, occurrence := range occurrences {
				inputsErr.Append(errors.New(occurrence))
			}
			errs.AppendWithPrefixf(inputsErr, `input "%s" is defined %d times in`, inputID, len(occurrences))
		}
	}

	// Check all referenced inputs exist
	inputsReferences.SortKeys(func(keys []string) {
		sort.Strings(keys)
	})
	for _, inputID := range inputsReferences.Keys() {
		if _, found := inputsOccurrences.Get(inputID); !found {
			// Referenced input is missing
			inputsErr := errors.NewMultiError()
			references, _ := inputsReferences.GetOrNil(inputID).([]string)
			for _, referencedFrom := range references {
				inputsErr.Append(errors.New(referencedFrom))
			}
			errs.AppendWithPrefixf(inputsErr, `input "%s" not found, referenced from`, inputID)
		}
	}

	// Check multi-input rules
	for _, input := range inputsMap {
		// Check that input kind=KindOAuthAccounts is defined for a supported component
		if input.Kind == KindOAuthAccounts {
			if oauthInput, found := inputsMap[input.OauthInputID]; found {
				if !OauthAccountsSupportedComponents[oauthInput.ComponentID] {
					errs.Append(errors.Errorf(`input "%s" (kind=%s) is defined for "%s" component, but it is not supported`, input.ID, input.Kind, oauthInput.ComponentID))
				}
			}
		}
	}

	// Validate other rules
	if err := validateDefinitions(ctx, g); err != nil {
		errs.Append(err)
	}

	// Enhance error messages
	enhancedErrors := errors.NewMultiError()
	for _, item := range errs.WrappedErrors() {
		msg := item.Error()

		// Replace step and group by index. Example:
		//   before: [0].steps[0].inputs[0].default
		//   after:  group 1, step 1, input "foo.bar": default
		regex := regexpcache.MustCompile(`^"\[(\d+)\](?:\.steps\[(\d+)\])?(?:\.inputs\[(\d+)\])?\.([^"]+)"`)
		match := regex.FindStringSubmatch(msg)
		if match == nil {
			enhancedErrors.Append(errors.New(msg))
			continue
		}

		msg = regex.ReplaceAllStringFunc(msg, func(s string) string {
			var out strings.Builder

			// Group index
			groupIndex := cast.ToInt(match[1])
			out.WriteString("group ")
			out.WriteString(cast.ToString(groupIndex + 1))

			// Step index
			var stepIndex int
			if match[2] != "" {
				stepIndex = cast.ToInt(match[2])
				out.WriteString(", step ")
				out.WriteString(cast.ToString(stepIndex + 1))
			}

			// Input ID
			if match[3] != "" {
				inputIndex := cast.ToInt(strings.Trim(match[3], "[]."))
				out.WriteString(`, input "`)
				out.WriteString(g[groupIndex].Steps[stepIndex].Inputs.GetIndex(inputIndex).ID)
				out.WriteString(`"`)
			}

			field := match[4]
			out.WriteString(": ")
			out.WriteString(`"`)
			out.WriteString(field)
			out.WriteString(`"`)
			return out.String()
		})

		msg = strings.Replace(msg, `"steps" must contain at least 1 item`, `"steps" must contain at least 1 step`, 1)
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
	Icon              string  `json:"icon" validate:"required,templateicon,min=1"`
	Name              string  `json:"name" validate:"required,min=1,max=25"`
	Description       string  `json:"description" validate:"min=1,max=60"`
	DialogName        string  `json:"dialogName,omitempty" validate:"omitempty,max=25"`
	DialogDescription string  `json:"dialogDescription,omitempty" validate:"omitempty,mdmax=200"`
	Backend           *string `json:"backend,omitempty"`
	Inputs            Inputs  `json:"inputs" validate:"omitempty,dive"`
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

// MatchesAvailableBackend checks whether the Input's backend is compatible
// with the provided list of available backends. If the Input's Backend is
// empty (""), or it matches one of the backends in the provided list, the
// function returns true. Otherwise, it returns false.
func (s Step) MatchesAvailableBackend(backends []string) bool {
	if s.Backend == nil || slices.Contains(backends, *s.Backend) {
		return true
	}

	return false
}
