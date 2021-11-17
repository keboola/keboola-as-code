package orchestrator

import "github.com/keboola/keboola-as-code/internal/pkg/model"

type orchestratorMapper struct {
	model.MapperContext
}

func NewMapper(context model.MapperContext) *orchestratorMapper {
	return &orchestratorMapper{MapperContext: context}
}

func (m *orchestratorMapper) isOrchestratorConfig(object model.Object) (bool, error) {
	config, ok := object.(*model.Config)
	if !ok {
		return false, nil
	}

	component, err := m.State.Components().Get(config.ComponentKey())
	if err != nil {
		return false, err
	}

	return component.IsOrchestrator(), nil
}

func (m *orchestratorMapper) isOrchestratorConfigState(objectState model.ObjectState) (bool, error) {
	config, ok := objectState.(*model.ConfigState)
	if !ok {
		return false, nil
	}

	component, err := m.State.Components().Get(config.ComponentKey())
	if err != nil {
		return false, err
	}

	return component.IsOrchestrator(), nil
}
