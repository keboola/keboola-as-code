package template

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type _state = *state.State

type State struct {
	_state
	template *EvaluatedTemplate
}

func NewState(s *state.State, template *EvaluatedTemplate) *State {
	return &State{_state: s, template: template}
}

func (s *State) Fs() filesystem.Fs {
	return s.template.Fs()
}

func (s *State) TemplateManifest() *Manifest {
	return s.template.TemplateManifest()
}
