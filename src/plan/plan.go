package plan

import (
	"keboola-as-code/src/diff"
	"keboola-as-code/src/state"
)

// Plan of the operation: pull, push, ...
type Plan struct {
	Name         string
	CurrentState *state.State
	Actions      []*Action
}

func (p *Plan) Add(d *diff.Result, t ActionType) {
	p.Actions = append(p.Actions, &Action{d, t})
}
