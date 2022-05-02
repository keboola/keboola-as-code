package template

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type _state = *state.State

type State struct {
	_state
	container *evaluatedTemplate
}

func NewState(s *state.State, container *evaluatedTemplate) *State {
	return &State{_state: s, container: container}
}

func (s *State) Fs() filesystem.Fs {
	return s.container.Fs()
}

func (s *State) TemplateManifest() *Manifest {
	return s.container.TemplateManifest()
}

func (s *State) MainConfig() (*model.ConfigKey, error) {
	return s.container.MainConfig()
}
