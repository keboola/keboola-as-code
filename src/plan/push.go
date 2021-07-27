package plan

import (
	"fmt"
	"keboola-as-code/src/diff"
	"keboola-as-code/src/model"
	"keboola-as-code/src/state"
)

func Push(diffResults *diff.Results) *Plan {
	plan := &Plan{Name: "push", CurrentState: diffResults.CurrentState}
	for _, result := range diffResults.Results {
		switch result.State {
		case diff.ResultEqual:
			// nop
		case diff.ResultNotEqual:
			plan.Add(result, ActionSaveRemote)
		case diff.ResultOnlyInLocal:
			plan.Add(result, ActionSaveRemote)
		case diff.ResultOnlyInRemote:
			if parentExists(result.ObjectState, plan.CurrentState) {
				plan.Add(result, ActionDeleteRemote)
			}
		case diff.ResultNotSet:
			panic(fmt.Errorf("diff was not generated"))
		}
	}

	return plan
}

func parentExists(objectState model.ObjectState, currentState *state.State) bool {
	switch v := objectState.(type) {
	case *model.BranchState:
		return true
	case *model.ConfigState:
		config := v.Remote
		branch := currentState.GetBranch(*config.BranchKey(), false)
		return branch.Local != nil
	case *model.ConfigRowState:
		row := v.Remote
		config := currentState.GetConfig(*row.ConfigKey(), false)
		branch := currentState.GetBranch(*config.BranchKey(), false)
		return config.Local != nil && branch.Local != nil

	default:
		panic(fmt.Errorf(`unexpected type "%T"`, objectState))
	}
}
