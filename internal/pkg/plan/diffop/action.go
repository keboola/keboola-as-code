package diffop

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

const (
	ActionSaveLocal ActionType = iota
	ActionSaveRemote
	ActionDeleteLocal
	ActionDeleteRemote
)

type ActionType int

// action on the diff result.
type action struct {
	*diff.Result
	action ActionType
}

func (a *action) String() string {
	msg := a.markString() + " " + a.Kind().Abbr + " " + a.Path()
	if !a.ChangedFields.IsEmpty() {
		msg += " | changed: " + a.ChangedFields.String()
	}
	return msg
}

func (a *action) StringVerbose() string {
	return a.opString() + " " + a.Kind().Name + " \"" + a.Path() + "\""
}

func (a *action) opString() string {
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

func (a *action) markString() string {
	switch a.State {
	case diff.ResultNotSet:
		return "?"
	case diff.ResultNotEqual:
		return diff.ChangeMark
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

func (a *action) validate() error {
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
