package scheduler

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func (m *schedulerMapper) MapAfterRemoteLoad(recipe *model.RemoteLoadRecipe) error {
	// Scheduler is a config
	internalObject, ok := recipe.InternalObject.(*model.Config)
	if !ok {
		return nil
	}

	// Check component type
	component, err := m.State.Components().Get(internalObject.ComponentKey())
	if err != nil {
		return err
	}
	if !component.IsScheduler() {
		return nil
	}

	// Target is stored in configuration
	targetRaw, found := internalObject.Content.Get(model.SchedulerTargetKey)
	if !found {
		return nil
	}

	// Target must be JSON object
	target, ok := targetRaw.(*orderedmap.OrderedMap)
	if !ok {
		return nil
	}

	// Component ID must be present
	componentIdRaw, found := target.Get(model.SchedulerTargetComponentIdKey)
	if !found {
		return nil
	}

	// Component ID must be string
	componentId, ok := componentIdRaw.(string)
	if !ok {
		return nil
	}

	// Configuration ID must be present
	configurationIdRaw, found := target.Get(model.SchedulerTargetConfigurationIdKey)
	if !found {
		return nil
	}

	// Configuration ID must be string
	configurationId, ok := configurationIdRaw.(string)
	if !ok {
		return nil
	}

	// Create relation
	internalObject.AddRelation(&model.SchedulerForRelation{
		ComponentId: model.ComponentId(componentId),
		ConfigId:    model.ConfigId(configurationId),
	})

	// Remove component and configuration ID
	target.Delete(model.SchedulerTargetComponentIdKey)
	target.Delete(model.SchedulerTargetConfigurationIdKey)
	internalObject.Content.Set(model.SchedulerTargetKey, target)
	return nil
}
