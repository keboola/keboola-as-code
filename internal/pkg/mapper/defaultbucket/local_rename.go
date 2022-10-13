package defaultbucket

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// onObjectsRename - find renamed configurations that are used in default buckets placeholders.
func (m *defaultBucketMapper) onObjectsRename(renamed []model.RenameAction, allObjects model.Objects) error {
	// Find renamed configurations used in IM.
	objectsToUpdate := make(map[string]model.Key)
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

		for _, relationRaw := range localConfig.Relations.GetByType(model.UsedInConfigInputMappingRelType) {
			relation := relationRaw.(*model.UsedInConfigInputMappingRelation)
			objectsToUpdate[relation.UsedIn.String()] = relation.UsedIn
		}
		for _, relationRaw := range localConfig.Relations.GetByType(model.UsedInRowInputMappingRelType) {
			relation := relationRaw.(*model.UsedInRowInputMappingRelation)
			objectsToUpdate[relation.UsedIn.String()] = relation.UsedIn
		}
	}

	// Log and save
	uow := m.state.LocalManager().NewUnitOfWork(context.Background())
	errs := errors.NewMultiError()
	if len(objectsToUpdate) > 0 {
		m.logger.Debug(`Need to update configurations:`)
		for _, key := range objectsToUpdate {
			m.logger.Debugf(`  - %s`, key.Desc())
			objectState := m.state.MustGet(key)
			uow.SaveObject(objectState, objectState.LocalState(), model.NewChangedFields(`configuration`))
		}
	}

	// Invoke
	if err := uow.Invoke(); err != nil {
		errs.Append(err)
	}

	return errs.ErrorOrNil()
}
