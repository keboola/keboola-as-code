package plan

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
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
		branch, branchFound := currentState.Get(config.BranchKey())
		return branchFound && branch.HasLocalState()
	case *model.ConfigRowState:
		row := v.Remote
		config, configFound := currentState.Get(row.ConfigKey())
		branch, branchFound := currentState.Get(row.BranchKey())
		return configFound && config.HasLocalState() && branchFound && branch.HasLocalState()

	default:
		panic(fmt.Errorf(`unexpected type "%T"`, objectState))
	}
}
