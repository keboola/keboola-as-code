package plan

import (
	"fmt"
	"strings"

	"keboola-as-code/src/diff"
	"keboola-as-code/src/model"
	"keboola-as-code/src/state"
)

const (
	ActionSaveLocal DiffActionType = iota
	ActionSaveRemote
	ActionDeleteLocal
	ActionDeleteRemote
	ActionMarkDeletedRemote
)

type DiffActionType int

// DiffAction - an action on the diff result.
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

	return nil
}
