package plan

import (
	"fmt"
	"keboola-as-code/src/diff"
)

func Pull(diffResults *diff.Results) *Plan {
	plan := &Plan{Name: "pull", CurrentState: diffResults.CurrentState}
	for _, result := range diffResults.Results {
		switch result.State {
		case diff.ResultEqual:
			// nop
		case diff.ResultNotEqual:
			plan.Add(result, ActionSaveLocal)
		case diff.ResultOnlyInLocal:
			plan.Add(result, ActionDeleteLocal)
		case diff.ResultOnlyInRemote:
			plan.Add(result, ActionSaveLocal)
		case diff.ResultNotSet:
			panic(fmt.Errorf("diff was not generated"))
		}
	}

	return plan
}
