package plan

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
)

func Pull(diffResults *diff.Results) (*DiffPlan, error) {
	plan := &DiffPlan{name: "pull", State: diffResults.CurrentState}

	for _, result := range diffResults.Results {
		switch result.State {
		case diff.ResultEqual:
			// nop
		case diff.ResultNotEqual:
			// SKIP: if only Relations have changed + no changed relations on the local side
			if result.ChangedFields.String() == "relations" && !result.ChangedFields.Get("relations").HasPath("InManifest") {
				continue
			}
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
