package orchestrator

import (
	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type orchestratorMapper struct {
	model.MapperContext
	localManager *local.Manager
}

func NewMapper(localManager *local.Manager, context model.MapperContext) *orchestratorMapper {
	return &orchestratorMapper{MapperContext: context, localManager: localManager}
}

func (m *orchestratorMapper) isOrchestratorConfigKey(key model.Key) (bool, error) {
	config, ok := key.(model.ConfigKey)
	if !ok {
		return false, nil
	}

	component, err := m.State.Components().Get(config.ComponentKey())
	if err != nil {
		return false, err
	}

	return component.IsOrchestrator(), nil
}
