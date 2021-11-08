package scheduler

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (m *schedulerMapper) MapBeforePersist(recipe *model.PersistRecipe) error {
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
	component, err := m.State.Components().Get(configManifest.ComponentKey())
	if err != nil {
		return err
	}

	// Component must be "scheduler"
	if !component.IsScheduler() {
		return nil
	}

	// Branch must be same
	if configKey.BranchKey() != configManifest.BranchKey() {
		panic(fmt.Errorf(`child "%s" and parent "%s" must be from same branch`, configManifest.Desc(), configKey.Desc()))
	}

	// Add relation
	configManifest.Relations.Add(&model.SchedulerForRelation{
		ComponentId: configKey.ComponentId,
		ConfigId:    configKey.Id,
	})

	return nil
}
