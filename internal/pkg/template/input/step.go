package input

import (
	"fmt"
	"sort"

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

type StepIndex struct {
	Step  int
	Group int
}

type StepsGroups []*StepsGroup

func (g StepsGroups) Indices(stepsToIds map[StepIndex]string) map[string]StepIndex {
	res := make(map[string]StepIndex)
	for gIdx, group := range g {
		for sIdx := range group.Steps {
			index := StepIndex{
				Step:  sIdx,
				Group: gIdx,
			}
			res[stepsToIds[index]] = index
		}
	}
	return res
}

func (g StepsGroups) AddInput(input Input, index StepIndex) error {
	if len(g) < index.Group {
		return fmt.Errorf("group at index %d not found", index.Group)
	}
	if len(g[index.Group].Steps) < index.Step {
		return fmt.Errorf("step at index %d for group at index %d not found", index.Step, index.Group)
	}
	g[index.Group].Steps[index.Step].Inputs = append(g[index.Group].Steps[index.Step].Inputs, input)
	return nil
}

func (g StepsGroups) InputsForStep(index StepIndex) (Inputs, bool) {
	if len(g) < index.Group {
		return nil, false
	}
	if len(g[index.Group].Steps) < index.Step {
		return nil, false
	}
	return g[index.Group].Steps[index.Step].Inputs, true
}

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

func (g StepsGroups) Validate() *utils.MultiError {
	errors := utils.NewMultiError()

	// Check duplicate inputs
	inputsOccurrences := orderedmap.New()
	g.VisitInputs(func(gIndex, sIndex int, group *StepsGroup, step *Step, input Input) {
		vRaw, _ := inputsOccurrences.Get(input.Id)
		v, _ := vRaw.([]string)
		v = append(v, fmt.Sprintf(`step "%s" (g%02d-s%02d)`, step.Name, gIndex+1, sIndex+1))
		inputsOccurrences.Set(input.Id, v)
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

	if errors.Len() > 0 {
		return errors
	}
	return nil
}

type VisitInputsCallback func(gIndex, sIndex int, group *StepsGroup, step *Step, input Input)

func (g StepsGroups) VisitInputs(fn VisitInputsCallback) {
	for gIndex, group := range g {
		for sIndex, step := range group.Steps {
			for _, input := range step.Inputs {
				fn(gIndex, sIndex, group, step, input)
			}
		}
	}
}

// StepsGroup is a container for Steps.
type StepsGroup struct {
	Description string `json:"description" validate:"min=1,max=80"`
	Required    string `json:"required" validate:"oneof=all atLeastOne exactlyOne zeroOrOne optional"`
	Steps       Steps  `json:"steps" validate:"min=1,dive"`
}

const (
	requiredAll                   = "all"
	requiredAtLeastOne            = "atLeastOne"
	requiredExactlyOne            = "exactlyOne"
	requiredZeroOrOne             = "zeroOrOne"
	requiredAtLeastOneDescription = "at least one step must be selected"
	requiredExactlyOneDescription = "exactly one step must be selected"
	requiredZeroOrOneDescription  = "zero or one step must be selected"
)

func (g StepsGroup) ShowStepsSelect() bool {
	return g.Required != requiredAll &&
		(len(g.Steps) > 1 || (g.Required != requiredAtLeastOne && g.Required != requiredExactlyOne))
}

func (g StepsGroup) ValidateSelectedSteps(selected int) error {
	if g.Required == requiredAtLeastOne && selected < 1 {
		return fmt.Errorf(requiredAtLeastOneDescription)
	}
	if g.Required == requiredExactlyOne && selected != 1 {
		return fmt.Errorf(requiredExactlyOneDescription)
	}
	if g.Required == requiredZeroOrOne && selected > 1 {
		return fmt.Errorf(requiredZeroOrOneDescription)
	}
	return nil
}

type Steps []*Step

func (s Steps) SelectOptions() []string {
	res := make([]string, 0)
	for _, step := range s {
		res = append(res, fmt.Sprintf("%s - %s", step.Name, step.Description))
	}
	return res
}

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
