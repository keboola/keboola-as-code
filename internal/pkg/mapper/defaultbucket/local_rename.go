package defaultbucket

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// onObjectsRename - find renamed configurations that are used in default buckets placeholders.
func (m *defaultBucketMapper) onObjectsRename(renamed []model.RenameAction, allObjects *model.StateObjects) error {
	// Find renamed configurations used in IM.
	configurationsToUpdate := make(map[string]model.Key)
	for _, object := range renamed {
		manifest, ok := object.Manifest.(*model.ConfigManifest)
		if !ok {
			continue
		}

		localConfigRaw, found := allObjects.Get(manifest.Key())
		if !found {
			continue
		}
		localConfig := localConfigRaw.(*model.Config)

		relations := localConfig.Relations.GetByType(model.UsedInInputMappingRelType)
		for _, relationRaw := range relations {
			relation := relationRaw.(*model.UsedInInputMappingRelation)
			configurationsToUpdate[relation.ConfigKey.String()] = relation.ConfigKey
		}
	}

	// Log and save
	uow := m.localManager.NewUnitOfWork(context.Background())
	errors := utils.NewMultiError()
	if len(configurationsToUpdate) > 0 {
		m.Logger.Debug(`Need to update configurations:`)
		for _, key := range configurationsToUpdate {
			m.Logger.Debugf(`  - %s`, key.Desc())
			configState := m.State.MustGet(key)
			uow.SaveObject(configState, configState.LocalState(), model.NewChangedFields(`configuration`))
		}
	}

	// Invoke
	if err := uow.Invoke(); err != nil {
		errors.Append(err)
	}

	return errors.ErrorOrNil()
}
