package scheduler

import (
	"context"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type mapper struct {
	dependencies
}

type dependencies interface {
}

func NewMapper() *mapper {
	return &mapper{}
}

func (m *mapper) MapAfterRemoteLoad(ctx context.Context, recipe *model.RemoteLoadRecipe) error {
	// Scheduler is a config
	object, ok := recipe.Object.(*model.Config)
	if !ok {
		return nil
	}

	// Check component type
	component, err := m.state.Components().GetOrErr(object.ComponentId)
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
	object.AddRelation(&model.SchedulerForRelation{
		ComponentId: storageapi.ComponentID(componentId),
		ConfigId:    storageapi.ConfigID(configurationId),
	})

	// Remove component and configuration ID
	target.Delete(model.SchedulerTargetComponentIdKey)
	target.Delete(model.SchedulerTargetConfigurationIdKey)
	object.Content.Set(model.SchedulerTargetKey, target)
	return nil
}
