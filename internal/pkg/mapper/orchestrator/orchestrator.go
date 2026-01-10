package orchestrator

import (
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
}

type orchestratorMapper struct {
	dependencies
	state  *state.State
	logger log.Logger
}

func NewMapper(s *state.State, d dependencies) *orchestratorMapper {
	return &orchestratorMapper{state: s, logger: s.Logger(), dependencies: d}
}

func (m *orchestratorMapper) isOrchestratorConfigKey(key model.Key) (bool, error) {
	config, ok := key.(model.ConfigKey)
	if !ok {
		return false, nil
	}

	component, err := m.state.Components().GetOrErr(config.ComponentID)
	if err != nil {
		return false, err
	}

	return component.IsOrchestrator(), nil
}

func markConfigUsedInOrchestrator(targetConfig, orchestratorConfig *model.Config) {
	targetConfig.Relations.Add(&model.UsedInOrchestratorRelation{
		ConfigID: orchestratorConfig.ID,
	})
}
