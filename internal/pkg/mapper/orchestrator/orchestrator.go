package orchestrator

import (
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

// FlowComponentID is the component ID for Keboola Flow (newer orchestration component).
const FlowComponentID = keboola.ComponentID("keboola.flow")

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

	return IsOrchestratorOrFlow(component), nil
}

// IsOrchestratorOrFlow returns true if the component is an orchestrator or a flow.
// Both keboola.orchestrator and keboola.flow are treated similarly for inline schedules.
func IsOrchestratorOrFlow(component *keboola.Component) bool {
	return component.IsOrchestrator() || component.ComponentKey.ID == FlowComponentID
}

func markConfigUsedInOrchestrator(targetConfig, orchestratorConfig *model.Config) {
	targetConfig.Relations.Add(&model.UsedInOrchestratorRelation{
		ConfigID: orchestratorConfig.ID,
	})
}
