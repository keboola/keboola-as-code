package plan

import (
	"fmt"
	"keboola-as-code/src/diff"
	"keboola-as-code/src/model"
	"keboola-as-code/src/state"
	"strings"
)

const (
	ActionSaveLocal DiffActionType = iota
	ActionSaveRemote
	ActionDeleteLocal
	ActionDeleteRemote
)

type DiffActionType int

// DiffAction - an action on the diff result
type DiffAction struct {
	*diff.Result
	action DiffActionType
}

func (a *DiffAction) String() string {
	msg := a.markString() + " " + a.Kind().Abbr + " " + a.RelativePath()
	if len(a.ChangedFields) > 0 {
		msg += " | changed: " + strings.Join(a.ChangedFields, ", ")
	}
	return msg
}

func (a *DiffAction) StringVerbose() string {
	return a.opString() + " " + a.Kind().Name + " \"" + a.RelativePath() + "\""
}

func (a *DiffAction) opString() string {
	switch a.State {
	case diff.ResultNotEqual:
		return "update"
	default:
		if a.action == ActionSaveLocal || a.action == ActionSaveRemote {
			return "create"
		} else {
			return "delete"
		}
	}
}

func (a *DiffAction) markString() string {
	switch a.State {
	case diff.ResultNotSet:
		return "? "
	case diff.ResultNotEqual:
		return "CH"
	case diff.ResultEqual:
		return "= "
	default:
		if a.action == ActionSaveLocal || a.action == ActionSaveRemote {
			return "+ "
		} else {
			return "× "
		}
	}
}

func (a *DiffAction) validate(currentState *state.State) error {
	// Branch rules
	if branchState, ok := a.ObjectState.(*model.BranchState); ok {
		// Default branch cannot be delete
		if a.action == ActionDeleteRemote {
			branch := branchState.Remote
			if branch.IsDefault {
				return fmt.Errorf("cannot %s, default branch can never be deleted", a.StringVerbose())
			} else {
				return fmt.Errorf("cannot %s, branch cannot be deleted by CLI", a.StringVerbose())
			}
		}
	}

	// Config rules
	if configState, ok := a.ObjectState.(*model.ConfigState); ok {
		// Config from dev-branch cannot be removed, it can be only marked for removal
		if a.action == ActionDeleteRemote {
			config := configState.Remote
			branch := currentState.Get(*config.BranchKey()).RemoteState().(*model.Branch)
			if !branch.IsDefault {
				return fmt.Errorf("cannot %s from dev branch", a.StringVerbose())
			}
		}
	}

	// Config row rules
	if configRowState, ok := a.ObjectState.(*model.ConfigRowState); ok {
		// Config row from dev-branch cannot be removed, it can be only marked for removal
		if a.action == ActionDeleteRemote {
			row := configRowState.Remote
			config := currentState.Get(*row.ConfigKey()).RemoteState().(*model.Config)
			branch := currentState.Get(*config.BranchKey()).RemoteState().(*model.Branch)
			if !branch.IsDefault {
				return fmt.Errorf("cannot %s from dev branch", a.StringVerbose())
			}
		}
	}

	return nil
}
