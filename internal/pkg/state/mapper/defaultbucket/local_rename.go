package defaultbucket

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
)

// AfterLocalRename find renamed configurations that are used in default buckets placeholders.
func (m *defaultBucketMapper) AfterLocalRename(state *local.State, changes []model.RenameAction) error {
	// Find renamed configurations used in IM.
	objectsToUpdate := make(map[model.Key]bool)
	for _, item := range changes {
		key, ok := item.Key.(model.ConfigKey)
		if !ok {
			continue
		}

		config := state.MustGet(key).(*model.Config)
		for _, relationRaw := range config.Relations.GetByType(model.UsedInConfigInputMappingRelType) {
			relation := relationRaw.(*model.UsedInConfigInputMappingRelation)
			objectsToUpdate[relation.UsedIn] = true
		}
		for _, relationRaw := range config.Relations.GetByType(model.UsedInRowInputMappingRelType) {
			relation := relationRaw.(*model.UsedInRowInputMappingRelation)
			objectsToUpdate[relation.UsedIn] = true
		}
	}

	// Log and save
	if len(objectsToUpdate) > 0 {
		m.logger.Debug(`Need to update configurations:`)
		uow := state.NewUnitOfWork(context.Background())
		for key := range objectsToUpdate {
			m.logger.Debugf(`  - %s`, key.String())
			uow.Save(state.MustGet(key), model.NewChangedFields(`configuration`))
		}

		// Invoke
		return uow.Invoke()
	}

	return nil
}
