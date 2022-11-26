package scheduler

import (
	"context"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m *schedulerMapper) MapBeforeRemoteSave(ctx context.Context, recipe *model.RemoteSaveRecipe) error {
	// Scheduler is a config
	object, ok := recipe.Object.(*model.Config)
	if !ok {
		return nil
	}

	// Get relation
	relType := model.SchedulerForRelType
	relationRaw, err := object.Relations.GetOneByType(relType)
	if err != nil {
		return errors.Errorf(`unexpected state of %s: %w`, recipe.Desc(), err)
	} else if relationRaw == nil {
		return nil
	}
	relation := relationRaw.(*model.SchedulerForRelation)

	// Get target
	targetRaw, found := object.Content.Get(model.SchedulerTargetKey)
	target, ok := targetRaw.(*orderedmap.OrderedMap)
	if !found {
		return errors.NewNestedError(
			errors.Errorf(`scheduler %s is invalid`, recipe.Desc()),
			errors.Errorf(`key "%s" not found`, model.SchedulerTargetKey),
		)
	} else if !ok {
		return errors.NewNestedError(
			errors.Errorf(`scheduler %s is invalid`, recipe.Desc()),
			errors.Errorf(`key "%s" must be object, found "%T"`, model.SchedulerTargetKey, targetRaw),
		)
	}

	// Set componentId and configurationId
	target.Set(model.SchedulerTargetComponentIDKey, relation.ComponentID.String())
	target.Set(model.SchedulerTargetConfigurationIDKey, relation.ConfigID.String())
	object.Content.Set(model.SchedulerTargetKey, target)

	// Delete relation
	object.Relations.RemoveByType(relType)
	return nil
}
