package scheduler

import (
	"context"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (m *schedulerMapper) MapAfterRemoteLoad(ctx context.Context, recipe *model.RemoteLoadRecipe) error {
	// Scheduler is a config
	object, ok := recipe.Object.(*model.Config)
	if !ok {
		return nil
	}

	// Check component type
	component, err := m.state.Components().GetOrErr(object.ComponentID)
	if err != nil {
		return err
	}
	if !component.IsScheduler() {
		return nil
	}

	// Target is stored in configuration
	targetRaw, found := object.Content.Get(model.SchedulerTargetKey)
	if !found {
		return nil
	}

	// Target must be JSON object
	target, ok := targetRaw.(*orderedmap.OrderedMap)
	if !ok {
		return nil
	}

	// Component ID must be present
	componentIDRaw, found := target.Get(model.SchedulerTargetComponentIDKey)
	if !found {
		return nil
	}

	// Component ID must be string
	componentID, ok := componentIDRaw.(string)
	if !ok {
		return nil
	}

	// Configuration ID must be present
	configurationIDRaw, found := target.Get(model.SchedulerTargetConfigurationIDKey)
	if !found {
		return nil
	}

	// Configuration ID must be string
	configurationID, ok := configurationIDRaw.(string)
	if !ok {
		return nil
	}

	// Create relation
	object.AddRelation(&model.SchedulerForRelation{
		ComponentID: keboola.ComponentID(componentID),
		ConfigID:    keboola.ConfigID(configurationID),
	})

	// Remove component and configuration ID
	target.Delete(model.SchedulerTargetComponentIDKey)
	target.Delete(model.SchedulerTargetConfigurationIDKey)
	object.Content.Set(model.SchedulerTargetKey, target)
	return nil
}
