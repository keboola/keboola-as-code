package pull

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/diffop"
)

func NewPlan(diffResults *diff.Results) (*diffop.Plan, error) {
	plan := diffop.NewPlan(`pull`)
	for _, result := range diffResults.Results {
		switch result.State {
		case diff.ResultEqual:
			// nop
		case diff.ResultNotEqual:
			// SKIP: if only Relations have changed + no changed relations on the local side
			if result.ChangedFields.String() == "relations" && !result.ChangedFields.Get("relations").HasPath("InManifest") {
				continue
			}
			plan.Add(result, diffop.ActionSaveLocal)
		case diff.ResultOnlyInLocal:
			plan.Add(result, diffop.ActionDeleteLocal)
		case diff.ResultOnlyInRemote:
			plan.Add(result, diffop.ActionSaveLocal)
		case diff.ResultNotSet:
			panic(fmt.Errorf("diff was not generated"))
		}
	}

	if err := plan.Validate(); err != nil {
		return nil, err
	}

	return plan, nil
}
