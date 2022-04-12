package orchestration

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type orchestratorLocalMapper struct {
	*helper
	logger log.Logger
}

type orchestratorRemoteMapper struct {
	*helper
	logger log.Logger
}

type helper struct {
	dependencies
}

type dependencies interface {
	Logger() log.Logger
	Components() (*model.ComponentsMap, error)
}

func NewLocalMapper(d dependencies) *orchestratorLocalMapper {
	return &orchestratorLocalMapper{helper: newHelper(d), logger: d.Logger()}
}

func NewRemoteMapper(d dependencies) *orchestratorRemoteMapper {
	return &orchestratorRemoteMapper{helper: newHelper(d), logger: d.Logger()}
}

func newHelper(d dependencies) *helper {
	return &helper{dependencies: d}
}

func (h *helper) isOrchestrator(key model.Key) (bool, error) {
	// Must be config
	configKey, ok := key.(model.ConfigKey)
	if !ok {
		return false, nil
	}

	// Get components
	components, err := h.Components()
	if err != nil {
		return false, err
	}

	// Get component
	component, err := components.Get(configKey.ComponentKey())
	if err != nil {
		return false, err
	}

	// Check
	if !component.IsOrchestrator() {
		return false, nil
	}

	return true, nil
}

func markConfigUsedInOrchestrator(targetConfig, orchestratorConfig *model.Config) {
	targetConfig.Relations.Add(&model.UsedInOrchestratorRelation{
		ConfigId: orchestratorConfig.ConfigId,
	})
}
