package plan

import (
	"fmt"

	"keboola-as-code/src/diff"
)

func Pull(diffResults *diff.Results) (*DiffPlan, error) {
	plan := &DiffPlan{name: "pull", State: diffResults.CurrentState}

	for _, result := range diffResults.Results {
		switch result.State {
		case diff.ResultEqual:
			// nop
		case diff.ResultInvisible:
			// nop
		case diff.ResultNotEqual:
			plan.add(result, ActionSaveLocal)
		case diff.ResultOnlyInLocal:
			plan.add(result, ActionDeleteLocal)
		case diff.ResultOnlyInRemote:
			plan.add(result, ActionSaveLocal)
		case diff.ResultNotSet:
			panic(fmt.Errorf("diff was not generated"))
		}
	}

	if err := plan.Validate(); err != nil {
		return nil, err
	}

	return plan, nil
}
