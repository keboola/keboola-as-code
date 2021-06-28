package plan

import (
	"keboola-as-code/src/diff"
	"keboola-as-code/src/state"
	"strings"
)

type ActionType int

const (
	ActionSaveLocal ActionType = iota
	ActionSaveRemote
	ActionDeleteLocal
	ActionDeleteRemote
)

// Plan of the operation: pull, push, ...
type Plan struct {
	Name         string
	CurrentState *state.State
	Actions      []*Action
}

// Action - one action from the Plan: add local config, delete remote branch, ...
type Action struct {
	*diff.Result
	Type ActionType
}

func (a *Action) String() string {
	msg := a.Mark() + " " + a.Kind().Abbr + " " + a.RelativePath()
	if len(a.ChangedFields) > 0 {
		msg += " | changed: " + strings.Join(a.ChangedFields, ", ")
	}
	return msg
}

func (a *Action) StringVerbose() string {
	return a.OpString() + " " + a.Kind().Name + " " + a.RelativePath()
}

func (a *Action) OpString() string {
	switch a.Result.State {
	case diff.ResultNotEqual:
		return "update"
	default:
		if a.Type == ActionSaveLocal || a.Type == ActionSaveRemote {
			return "create"
		} else {
			return "delete"
		}
	}
}

func (a *Action) Mark() string {
	switch a.Result.State {
	case diff.ResultNotSet:
		return "? "
	case diff.ResultNotEqual:
		return "CH"
	case diff.ResultEqual:
		return "= "
	default:
		if a.Type == ActionSaveLocal || a.Type == ActionSaveRemote {
			return "+ "
		} else {
			return "× "
		}
	}
}

func (p *Plan) Add(d *diff.Result, t ActionType) {
	p.Actions = append(p.Actions, &Action{d, t})
}
