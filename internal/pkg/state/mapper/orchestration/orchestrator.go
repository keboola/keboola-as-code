package orchestration

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/remote"
)

type orchestratorLocalMapper struct {
	*helper
	state  *local.State
	logger log.Logger
}

type orchestratorRemoteMapper struct {
	*helper
	state  *remote.State
	logger log.Logger
}

type helper struct {
	dependencies
	objects model.Objects
}

type dependencies interface {
	Logger() log.Logger
	Components() (*model.ComponentsMap, error)
}

func NewLocalMapper(s *local.State, d dependencies) *orchestratorLocalMapper {
	return &orchestratorLocalMapper{helper: newHelper(s, d), state: s, logger: d.Logger()}
}

func NewRemoteMapper(s *remote.State, d dependencies) *orchestratorRemoteMapper {
	return &orchestratorRemoteMapper{helper: newHelper(s, d), state: s, logger: d.Logger()}
}

func newHelper(objects model.Objects, d dependencies) *helper {
	return &helper{dependencies: d, objects: objects}
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
