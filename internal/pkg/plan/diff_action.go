package plan

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

const (
	ActionSaveLocal DiffActionType = iota
	ActionSaveRemote
	ActionDeleteLocal
	ActionDeleteRemote
)

type DiffActionType int

// DiffAction - an action on the diff result.
type DiffAction struct {
	*diff.Result
	action DiffActionType
}

func (a *DiffAction) String() string {
	msg := a.markString() + " " + a.Kind().Abbr + " " + a.Path()
	if !a.ChangedFields.IsEmpty() {
		msg += " | changed: " + a.ChangedFields.String()
	}
	return msg
}

func (a *DiffAction) StringVerbose() string {
	return a.opString() + " " + a.Kind().Name + " \"" + a.Path() + "\""
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
		return "?"
	case diff.ResultNotEqual:
		return diff.ChangedMark
	case diff.ResultEqual:
		return diff.EqualMark
	default:
		if a.action == ActionSaveLocal || a.action == ActionSaveRemote {
			return diff.AddMark
		} else {
			return diff.DeleteMark
		}
	}
}

func (a *DiffAction) validate() error {
	// Branch rules
	if branchState, ok := a.ObjectState.(*model.BranchState); ok {
		// Default branch cannot be delete
		if a.action == ActionDeleteRemote {
			branch := branchState.Remote
			if branch.IsDefault {
				return fmt.Errorf("cannot %s, default branch can never be deleted", a.StringVerbose())
			}
		}
	}

	return nil
}
