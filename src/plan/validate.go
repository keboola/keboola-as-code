package plan

import (
	"fmt"
	"keboola-as-code/src/model"
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"
)

func (p *Plan) Validate() error {
	errors := utils.NewMultiError()
	for _, action := range p.Actions {
		if err := action.validate(p.CurrentState); err != nil {
			errors.Append(err)
		}
	}
	return errors.ErrorOrNil()
}

func (a *Action) validate(currentState *state.State) error {
	// Branch rules
	if branchState, ok := a.ObjectState.(*model.BranchState); ok {
		// Default branch cannot be delete
		if a.Type == ActionDeleteRemote {
			branch := branchState.Remote
			if branch.IsDefault {
				return fmt.Errorf("cannot %s, default branch can never be deleted", a.StringVerbose())
			}
		}
	}

	// Config rules
	if configState, ok := a.ObjectState.(*model.ConfigState); ok {
		// Config from dev-branch cannot be removed, it can be only marked for removal
		if a.Type == ActionDeleteRemote {
			config := configState.Remote
			branch := currentState.GetBranch(*config.BranchKey(), false).Remote
			if !branch.IsDefault {
				return fmt.Errorf("cannot %s from dev branch", a.StringVerbose())
			}
		}
	}

	// Config row rules
	if configRowState, ok := a.ObjectState.(*model.ConfigRowState); ok {
		// Config row from dev-branch cannot be removed, it can be only marked for removal
		if a.Type == ActionDeleteRemote {
			row := configRowState.Remote
			config := currentState.GetConfig(*row.ConfigKey(), false).Remote
			branch := currentState.GetBranch(*config.BranchKey(), false).Remote
			if !branch.IsDefault {
				return fmt.Errorf("cannot %s from dev branch", a.StringVerbose())
			}
		}
	}

	return nil
}
