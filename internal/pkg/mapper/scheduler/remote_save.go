package scheduler

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func (m *schedulerMapper) MapBeforeRemoteSave(recipe *model.RemoteSaveRecipe) error {
	// Scheduler is a config
	object, ok := recipe.Object.(*model.Config)
	if !ok {
		return nil
	}

	// Get relation
	relType := model.SchedulerForRelType
	relationRaw, err := object.Relations.GetOneByType(relType)
	if err != nil {
		return fmt.Errorf(`unexpected state of %s: %w`, recipe.Desc(), err)
	} else if relationRaw == nil {
		return nil
	}
	relation := relationRaw.(*model.SchedulerForRelation)

	// Get target
	targetRaw, found := object.Content.Get(model.SchedulerTargetKey)
	target, ok := targetRaw.(*orderedmap.OrderedMap)
	if !found {
		return utils.PrefixError(
			fmt.Sprintf(`scheduler %s is invalid`, recipe.Desc()),
			fmt.Errorf(`key "%s" not found`, model.SchedulerTargetKey),
		)
	} else if !ok {
		return utils.PrefixError(
			fmt.Sprintf(`scheduler %s is invalid`, recipe.Desc()),
			fmt.Errorf(`key "%s" must be object, found "%T"`, model.SchedulerTargetKey, targetRaw),
		)
	}

	// Set componentId and configurationId
	target.Set(model.SchedulerTargetComponentIdKey, relation.ComponentId.String())
	target.Set(model.SchedulerTargetConfigurationIdKey, relation.ConfigId.String())
	object.Content.Set(model.SchedulerTargetKey, target)

	// Delete relation
	object.Relations.RemoveByType(relType)
	return nil
}
