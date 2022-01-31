package template

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type _state = *state.State

type State struct {
	_state
	container *ObjectsContainer
}

func NewState(s *state.State, container *ObjectsContainer) *State {
	return &State{_state: s, container: container}
}

func (s *State) Fs() filesystem.Fs {
	return s.container.Fs()
}

func (s *State) TemplateManifest() *Manifest {
	return s.container.TemplateManifest()
}
