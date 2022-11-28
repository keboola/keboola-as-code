package input

import (
	"fmt"

	"github.com/keboola/go-utils/pkg/orderedmap"
)

// StepsGroupsExt and nested structs are auxiliary structs with extended information about group/step.
type StepsGroupsExt []*StepsGroupExt

type StepsExt []*StepExt

type StepsGroupExt struct {
	StepsGroup
	Steps      StepsExt
	ID         string // eg. "g01"
	GroupIndex int
	Announced  bool // true if info about group has been printed in CLI dialog
}

type StepExt struct {
	Step
	ID         string // eg. "g01-s01"
	GroupIndex int
	StepIndex  int
	Show       bool // show in CLI dialog
	Announced  bool // true if info about step has been printed in CLI dialog
}

func (g StepsGroups) ToExtended() (groupsExt StepsGroupsExt) {
	for groupIndex, group := range g {
		groupID := fmt.Sprintf("g%02d", groupIndex+1)
		groupExt := &StepsGroupExt{StepsGroup: group, ID: groupID, GroupIndex: groupIndex}
		groupsExt = append(groupsExt, groupExt)
		for stepIndex, step := range group.Steps {
			stepID := fmt.Sprintf("%s-s%02d", groupID, stepIndex+1)
			stepExt := &StepExt{Step: step, ID: stepID, GroupIndex: groupIndex, StepIndex: stepIndex}
			groupExt.Steps = append(groupExt.Steps, stepExt)
		}
	}
	return groupsExt
}

func (g StepsGroupsExt) ToValue() (out StepsGroups) {
	for _, group := range g {
		groupOut := group.StepsGroup
		groupOut.Steps = make(Steps, 0)
		for _, step := range group.Steps {
			stepOut := step.Step
			if stepOut.Inputs == nil {
				stepOut.Inputs = make([]Input, 0)
			}
			groupOut.Steps = append(groupOut.Steps, stepOut)
		}
		out = append(out, groupOut)
	}
	return out
}

func (g StepsGroupsExt) ValidateDefinitions() error {
	return g.ToValue().ValidateDefinitions()
}

type VisitStepsCallback func(group *StepsGroupExt, step *StepExt) error

func (g StepsGroupsExt) VisitSteps(fn VisitStepsCallback) error {
	for _, group := range g {
		for _, step := range group.Steps {
			if err := fn(group, step); err != nil {
				return err
			}
		}
	}
	return nil
}

type VisitInputsCallback func(group *StepsGroupExt, step *StepExt, input *Input) error

func (g StepsGroupsExt) VisitInputs(fn VisitInputsCallback) error {
	return g.VisitSteps(func(group *StepsGroupExt, step *StepExt) error {
		for i := range step.Inputs {
			if err := fn(group, step, &step.Inputs[i]); err != nil {
				return err
			}
		}
		return nil
	})
}

// InputsMap returns inputId -> input map.
func (g StepsGroupsExt) InputsMap() map[string]*Input {
	out := make(map[string]*Input)
	_ = g.VisitInputs(func(_ *StepsGroupExt, _ *StepExt, input *Input) error {
		out[input.ID] = input
		return nil
	})
	return out
}

// StepsMap returns stepId -> step map.
func (g StepsGroupsExt) StepsMap() map[string]*StepExt {
	out := make(map[string]*StepExt)
	_ = g.VisitSteps(func(group *StepsGroupExt, step *StepExt) error {
		out[step.ID] = step
		return nil
	})
	return out
}

func (s StepsExt) OptionsForSelectBox() []string {
	res := make([]string, 0)
	for _, step := range s {
		res = append(res, fmt.Sprintf("%s - %s", step.Name, step.Description))
	}
	return res
}

func (s StepsExt) SelectedOptions() []int {
	res := make([]int, 0)
	for index, step := range s {
		if step.Show {
			res = append(res, index)
		}
	}
	return res
}

func (g *StepsGroupExt) AddStep(step *StepExt) {
	g.Steps = append(g.Steps, step)
}

func (s *StepExt) AddInput(input Input) {
	s.Inputs = append(s.Inputs, input)
}

// InputsMap - map of all Inputs by Input.ID.
type InputsMap struct {
	data *orderedmap.OrderedMap
}

func NewInputsMap() InputsMap {
	return InputsMap{data: orderedmap.New()}
}

func (v InputsMap) Add(input *Input) {
	v.data.Set(input.ID, input)
}

func (v InputsMap) Get(inputID string) (*Input, bool) {
	value, found := v.data.Get(inputID)
	if !found {
		return nil, false
	}
	return value.(*Input), true
}

func (v InputsMap) Ids() []string {
	return v.data.Keys()
}

func (v InputsMap) Sort(sortFunc func(inputsIds []string)) {
	v.data.SortKeys(sortFunc)
}

func (v InputsMap) All() Inputs {
	out := make(Inputs, v.data.Len())
	i := 0
	for _, key := range v.data.Keys() {
		item, _ := v.data.Get(key)
		out[i] = *(item.(*Input))
		i++
	}
	return out
}
