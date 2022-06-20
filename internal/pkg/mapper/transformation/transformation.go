package transformation

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type transformationMapper struct {
	state  *state.State
	logger log.Logger
}

func NewMapper(s *state.State) *transformationMapper {
	return &transformationMapper{state: s, logger: s.Logger()}
}

func (m *transformationMapper) isTransformationConfig(object interface{}) (bool, error) {
	v, ok := object.(*model.Config)
	if !ok {
		return false, nil
	}

	component, err := m.state.Components().GetOrErr(v.ComponentId)
	if err != nil {
		return false, err
	}

	return component.IsTransformation(), nil
}

func (m *transformationMapper) isTransformationConfigState(objectState model.ObjectState) (bool, error) {
	v, ok := objectState.(*model.ConfigState)
	if !ok {
		return false, nil
	}

	component, err := m.state.Components().GetOrErr(v.ComponentId)
	if err != nil {
		return false, err
	}

	return component.IsTransformation(), nil
}
