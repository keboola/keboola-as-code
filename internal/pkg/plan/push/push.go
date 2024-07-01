package push

import (
	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/diffop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func NewPlan(diffResults *diff.Results, allowTargetEnv bool) (*diffop.Plan, error) {
	plan := diffop.NewPlan(`push`)
	for _, result := range diffResults.Results {
		switch result.State {
		case diff.ResultEqual:
			// nop
		case diff.ResultNotEqual:
			// SKIP: if only Relations have changed + no changed relations on the API side
			if result.ChangedFields.String() == "relations" && !result.ChangedFields.Get("relations").HasPath("InAPI") {
				continue
			}
			plan.Add(result, diffop.ActionSaveRemote)
		case diff.ResultOnlyInLocal:
			plan.Add(result, diffop.ActionSaveRemote)
		case diff.ResultOnlyInRemote:
			if parentExists(result.ObjectState, diffResults.Objects) {
				plan.Add(result, diffop.ActionDeleteRemote)
			}
		case diff.ResultNotSet:
			panic(errors.New("diff was not generated"))
		}
	}

	if err := plan.Validate(); err != nil {
		return nil, err
	}

	return plan, nil
}

func parentExists(objectState model.ObjectState, objects model.ObjectStates) bool {
	switch v := objectState.(type) {
	case *model.BranchState:
		return true
	case *model.ConfigState:
		config := v.Remote
		branch, branchFound := objects.Get(config.BranchKey())
		return branchFound && branch.HasLocalState()
	case *model.ConfigRowState:
		row := v.Remote
		config, configFound := objects.Get(row.ConfigKey())
		branch, branchFound := objects.Get(row.BranchKey())
		return configFound && config.HasLocalState() && branchFound && branch.HasLocalState()

	default:
		panic(errors.Errorf(`unexpected type "%T"`, objectState))
	}
}
