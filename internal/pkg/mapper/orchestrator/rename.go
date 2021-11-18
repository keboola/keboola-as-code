package orchestrator

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (m *orchestratorMapper) OnObjectsRename(event model.OnObjectsRenameEvent) error {
	errors := utils.NewMultiError()
	localObjects := m.State.LocalObjects()

	// Find renamed orchestrators and renamed configs used in an orchestrator
	orchestratorsToUpdate := make(map[string]model.Key)
	for _, object := range event.RenamedObjects {
		key := object.Record.Key()

		// Is orchestrator?
		if ok, err := m.isOrchestratorConfigKey(key); err != nil {
			errors.Append(err)
			continue
		} else if ok {
			orchestratorsToUpdate[key.String()] = key
			continue
		}

		// Is config used in orchestrator?
		if manifest, ok := object.Record.(*model.ConfigManifest); ok {
			localConfigRaw, found := localObjects.Get(manifest.Key())
			if found {
				localConfig := localConfigRaw.(*model.Config)
				relations := localConfig.Relations.GetByType(model.UsedInOrchestratorRelType)
				for _, relationRaw := range relations {
					relation := relationRaw.(*model.UsedInOrchestratorRelation)
					orchestratorKey := model.ConfigKey{
						BranchId:    localConfig.BranchId,
						ComponentId: model.OrchestratorComponentId,
						Id:          relation.ConfigId,
					}
					orchestratorsToUpdate[orchestratorKey.String()] = orchestratorKey
				}
			}
		}
	}

	// Log and save
	uow := m.localManager.NewUnitOfWork(context.Background())
	if len(orchestratorsToUpdate) > 0 {
		m.Logger.Debug(`Need to update orchestrators:`)
		for _, key := range orchestratorsToUpdate {
			m.Logger.Debugf(`  - %s`, key.Desc())
			orchestrator := m.State.MustGet(key)
			uow.SaveObject(orchestrator, orchestrator.LocalState(), model.NewChangedFields(`orchestrator`))
		}
	}

	// Invoke
	if err := uow.Invoke(); err != nil {
		errors.Append(err)
	}

	return errors.ErrorOrNil()
}
