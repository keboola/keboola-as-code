package orchestrator

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (m *orchestratorMapper) OnObjectsLoad(event model.OnObjectsLoadEvent) error {
	errors := utils.NewMultiError()
	for _, object := range event.NewObjects {
		// Object must be orchestrator config
		if ok, err := m.isOrchestratorConfigKey(object.Key()); err != nil {
			errors.Append(err)
			continue
		} else if !ok {
			continue
		}

		// Generate Orchestration struct
		config := object.(*model.Config)
		if event.StateType == model.StateTypeLocal {
			if err := m.onLocalLoad(config, event.AllObjects); err != nil {
				errors.Append(err)
			}
		} else if event.StateType == model.StateTypeRemote {
			m.onRemoteLoad(config, event.AllObjects)
		}
	}
	return errors.ErrorOrNil()
}

func markConfigUsedInOrchestrator(targetConfig, orchestratorConfig *model.Config) {
	targetConfig.Relations.Add(&model.UsedInOrchestratorRelation{
		ConfigId: orchestratorConfig.Id,
	})
}
