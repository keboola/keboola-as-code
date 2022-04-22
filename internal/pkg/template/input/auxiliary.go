package input

import (
	"fmt"
)

// StepsGroupsExt and nested structs are auxiliary structs with extended information about group/step.
type StepsGroupsExt []*StepsGroupExt

type StepsExt []*StepExt

type StepsGroupExt struct {
	StepsGroup `validate:"dive"`
	Steps      StepsExt
	Id         string // eg. "g01"
	GroupIndex int
	Announced  bool // true if info about group has been printed in CLI dialog
}

type StepExt struct {
	Step       `validate:"dive"`
	Id         string // eg. "g01-s01"
	GroupIndex int
	StepIndex  int
	Show       bool // show in CLI dialog
	Announced  bool // true if info about step has been printed in CLI dialog
}

func (g StepsGroups) ToExtended() (groupsExt StepsGroupsExt) {
	for groupIndex, group := range g {
		groupId := fmt.Sprintf("g%02d", groupIndex+1)
		groupExt := &StepsGroupExt{StepsGroup: *group, Id: groupId, GroupIndex: groupIndex}
		for stepIndex, step := range group.Steps {
			stepId := fmt.Sprintf("%s-s%02d", groupId, stepIndex+1)
			stepExt := &StepExt{Step: *step, Id: stepId, GroupIndex: groupIndex, StepIndex: stepIndex}
			groupExt.Steps = append(groupExt.Steps, stepExt)
		}
		groupsExt = append(groupsExt, groupExt)
	}
	return groupsExt
}

type VisitInputsCallback func(group *StepsGroupExt, step *StepExt, input Input) error

func (g StepsGroupsExt) VisitInputs(fn VisitInputsCallback) error {
	for _, group := range g {
		for _, step := range group.Steps {
			for _, input := range step.Inputs {
				if err := fn(group, step, input); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s StepsExt) OptionsForSelectBox() []string {
	res := make([]string, 0)
	for _, step := range s {
		res = append(res, fmt.Sprintf("%s - %s", step.Name, step.Description))
	}
	return res
}
