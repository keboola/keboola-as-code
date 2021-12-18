package transformation

import (
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type transformationMapper struct {
	mapper.Context
}

func NewMapper(context mapper.Context) *transformationMapper {
	return &transformationMapper{Context: context}
}

func (m *transformationMapper) isTransformationConfig(object interface{}) (bool, error) {
	v, ok := object.(*model.Config)
	if !ok {
		return false, nil
	}

	component, err := m.State.Components().Get(v.ComponentKey())
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

	component, err := m.State.Components().Get(v.ComponentKey())
	if err != nil {
		return false, err
	}

	return component.IsTransformation(), nil
}
