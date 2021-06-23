package plan

import (
	"fmt"
	"keboola-as-code/src/diff"
)

func Push(diffResults *diff.Results) *Plan {
	recipe := &Plan{Name: "push"}
	for _, result := range diffResults.Results {
		switch result.State {
		case diff.ResultEqual:
			// nop
		case diff.ResultNotEqual:
			recipe.Add(result, ActionSaveRemote)
		case diff.ResultOnlyInLocal:
			recipe.Add(result, ActionSaveRemote)
		case diff.ResultOnlyInRemote:
			recipe.Add(result, ActionDeleteRemote)
		case diff.ResultNotSet:
			panic(fmt.Errorf("diff was not generated"))
		}
	}
	return recipe
}
