package template

import (
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type _state = *state.State

type State struct {
	_state
	template *Template
}

func NewState(s *state.State, template *Template) *State {
	return &State{_state: s, template: template}
}
