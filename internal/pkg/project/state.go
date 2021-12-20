package project

import (
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type _state = *state.State

type State struct {
	_state
	project *Project
}

func NewState(s *state.State, project *Project) *State {
	return &State{_state: s, project: project}
}

func (s *State) State() *state.State {
	return s._state
}
