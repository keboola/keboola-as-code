package orchestrator

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// AfterLocalRename - find renamed orchestrators and renamed configs used in an orchestrator.
func (m *orchestratorLocalMapper) AfterLocalRename(changes []model.RenameAction) error {
	errors := utils.NewMultiError()

	// Find renamed orchestrators and renamed configs used in an orchestrator
	orchestratorsToUpdate := make(map[model.Key]bool)
	for _, item := range changes {
		key := item.Key

		// Is object an orchestrator?
		if ok, err := m.isOrchestrator(key); err != nil {
			errors.Append(err)
			continue
		} else if ok {
			orchestratorsToUpdate[key] = true
			continue
		}

		// Is object a config used in orchestrator?
		if _, ok := key.(model.ConfigKey); !ok {
			continue
		} else if configRaw, found := m.state.Get(key); found {
			config := configRaw.(*model.Config)
			relations := config.Relations.GetByType(model.UsedInOrchestratorRelType)
			for _, relationRaw := range relations {
				relation := relationRaw.(*model.UsedInOrchestratorRelation)
				orchestratorKey := model.ConfigKey{
					BranchId:    config.BranchId,
					ComponentId: model.OrchestratorComponentId,
					Id:          relation.ConfigId,
				}
				orchestratorsToUpdate[orchestratorKey] = true
			}
		}
	}

	// Log and save
	uow := m.state.NewUnitOfWork(context.Background(), model.NoFilter())
	if len(orchestratorsToUpdate) > 0 {
		m.logger.Debug(`Need to update orchestrators:`)
		for key := range orchestratorsToUpdate {
			m.logger.Debugf(`  - %s`, key.String())
			orchestrator := m.state.MustGet(key)
			uow.Save(orchestrator, model.NewChangedFields(`orchestration`))
		}
	}

	// Invoke
	if err := uow.Invoke(); err != nil {
		errors.Append(err)
	}

	return errors.ErrorOrNil()
}
