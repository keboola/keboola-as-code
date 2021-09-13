package plan

import (
	"fmt"

	"keboola-as-code/src/diff"
	"keboola-as-code/src/model"
	"keboola-as-code/src/state"
)

func Push(diffResults *diff.Results, changeDescription string) (*DiffPlan, error) {
	plan := &DiffPlan{
		name:              "push",
		changeDescription: changeDescription,
		State:             diffResults.CurrentState,
	}

	for _, result := range diffResults.Results {
		switch result.State {
		case diff.ResultEqual:
			// nop
		case diff.ResultInvisible:
			// nop
		case diff.ResultNotEqual:
			plan.add(result, ActionSaveRemote)
		case diff.ResultOnlyInLocal:
			plan.add(result, ActionSaveRemote)
		case diff.ResultOnlyInRemote:
			if parentExists(result.ObjectState, plan.State) {
				plan.add(result, ActionDeleteRemote)
			}
		case diff.ResultNotSet:
			panic(fmt.Errorf("diff was not generated"))
		}
	}

	if err := plan.Validate(); err != nil {
		return nil, err
	}

	return plan, nil
}

func parentExists(objectState model.ObjectState, currentState *state.State) bool {
	switch v := objectState.(type) {
	case *model.BranchState:
		return true
	case *model.ConfigState:
		config := v.Remote
		branch := currentState.Get(*config.BranchKey()).(*model.BranchState)
		return branch.HasLocalState()
	case *model.ConfigRowState:
		row := v.Remote
		config := currentState.Get(*row.ConfigKey()).(*model.ConfigState)
		branch := currentState.Get(*config.BranchKey()).(*model.BranchState)
		return config.HasLocalState() && branch.HasLocalState()

	default:
		panic(fmt.Errorf(`unexpected type "%T"`, objectState))
	}
}
