package scheduler

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m *schedulerMapper) MapBeforePersist(ctx context.Context, recipe *model.PersistRecipe) error {
	// Scheduler is represented by config
	configManifest, ok := recipe.Manifest.(*model.ConfigManifest)
	if !ok {
		return nil
	}

	// Parent of the scheduler must be target config
	configKey, ok := recipe.ParentKey.(model.ConfigKey)
	if !ok {
		return nil
	}

	// Get component
	component, err := m.state.Components().GetOrErr(configManifest.ComponentID)
	if err != nil {
		return err
	}

	// Component must be "scheduler"
	if !component.IsScheduler() {
		return nil
	}

	// Branch must be same
	if configKey.BranchKey() != configManifest.BranchKey() {
		panic(errors.Errorf(`child "%s" and parent "%s" must be from same branch`, configManifest.Desc(), configKey.Desc()))
	}

	// Add relation
	configManifest.Relations.Add(&model.SchedulerForRelation{
		ComponentID: configKey.ComponentID,
		ConfigID:    configKey.ID,
	})

	return nil
}
