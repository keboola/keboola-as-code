package input

import (
	"fmt"
)

type StepsGroups []*StepsGroup

type StepIndex struct {
	Step  int
	Group int
}

func (g StepsGroups) Indices() map[string]StepIndex {
	res := make(map[string]StepIndex)
	for gIdx, group := range g {
		for sIdx, step := range group.Steps {
			res[step.Id] = StepIndex{
				Step:  sIdx,
				Group: gIdx,
			}
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

func (g *StepsGroups) Validate() error {
	return validate(g)
}

type StepsGroup struct {
	Id          string  `json:"id"`
	Description string  `json:"description" validate:"max=80"`
	Required    string  `json:"required" validate:"oneof=all atLeastOne exactOne zeroOrOne optional"`
	Steps       []*Step `json:"steps" validate:"min=1,dive"`
}

type Step struct {
	Id                string `json:"id"`
	Icon              string `json:"icon" validate:"required"`
	Name              string `json:"name" validate:"required,max=20"`
	Description       string `json:"description" validate:"max=40"`
	DialogName        string `json:"dialogName,omitempty" validate:"omitempty,max=20"`
	DialogDescription string `json:"dialogDescription,omitempty" validate:"omitempty,max=200"`
	Inputs            Inputs `json:"inputs" validate:"omitempty,dive"`
}
