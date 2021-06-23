package plan

import (
	"keboola-as-code/src/diff"
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
	Name    string
	Actions []*Action
}

// Action - one action from the Plan: add local config, delete remote branch, ...
type Action struct {
	*diff.Result
	Type ActionType
}

func (a *Action) String() string {
	kindAbb := strings.ToUpper(string(a.Kind()[0]))
	msg := a.StringPrefix() + " " + kindAbb + " " + a.RelativePath()
	if len(a.ChangedFields) > 0 {
		msg += " | changed: " + strings.Join(a.ChangedFields, ", ")
	}
	return msg
}

func (a *Action) StringPrefix() string {
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
