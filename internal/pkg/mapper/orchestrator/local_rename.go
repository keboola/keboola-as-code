package orchestrator

import (
	"context"

	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// onObjectsRename - find renamed orchestrators and renamed configs used in an orchestrator.
func (m *orchestratorMapper) onObjectsRename(renamed []model.RenameAction, allObjects model.Objects) error {
	errors := utils.NewMultiError()

	// Find renamed orchestrators and renamed configs used in an orchestrator
	orchestratorsToUpdate := make(map[string]model.Key)
	for _, object := range renamed {
		key := object.Manifest.Key()

		// Is orchestrator?
		if ok, err := m.isOrchestratorConfigKey(key); err != nil {
			errors.Append(err)
			continue
		} else if ok {
			orchestratorsToUpdate[key.String()] = key
			continue
		}

		// Is config used in orchestrator?
		if manifest, ok := object.Manifest.(*model.ConfigManifest); ok {
			localConfigRaw, found := allObjects.Get(manifest.Key())
			if found {
				localConfig := localConfigRaw.(*model.Config)
				relations := localConfig.Relations.GetByType(model.UsedInOrchestratorRelType)
				for _, relationRaw := range relations {
					relation := relationRaw.(*model.UsedInOrchestratorRelation)
					orchestratorKey := model.ConfigKey{
						BranchId:    localConfig.BranchId,
						ComponentId: storageapi.OrchestratorComponentID,
						Id:          relation.ConfigId,
					}
					orchestratorsToUpdate[orchestratorKey.String()] = orchestratorKey
				}
			}
		}
	}

	// Log and save
	uow := m.state.LocalManager().NewUnitOfWork(context.Background())
	if len(orchestratorsToUpdate) > 0 {
		m.logger.Debug(`Need to update orchestrators:`)
		for _, key := range orchestratorsToUpdate {
			m.logger.Debugf(`  - %s`, key.Desc())
			orchestrator := m.state.MustGet(key)
			uow.SaveObject(orchestrator, orchestrator.LocalState(), model.NewChangedFields(`orchestrator`))
		}
	}

	// Invoke
	if err := uow.Invoke(); err != nil {
		errors.Append(err)
	}

	return errors.ErrorOrNil()
}
