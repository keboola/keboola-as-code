package transformation

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type transformationMapper struct {
	model.MapperContext
}

func NewMapper(context model.MapperContext) *transformationMapper {
	return &transformationMapper{MapperContext: context}
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
