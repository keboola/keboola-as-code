package scheduler

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
)

type localDependencies interface {
	Components() (*model.ComponentsMap, error)
}

type schedulerLocalMapper struct {
	localDependencies
	state *local.State
}

func NewLocalMapper(s *local.State, d localDependencies) *schedulerLocalMapper {
	return &schedulerLocalMapper{state: s, localDependencies: d}
}

func (m *schedulerLocalMapper) MapBeforePersist(recipe *model.PersistRecipe) error {
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

	// Get components
	components, err := m.Components()
	if err != nil {
		return err
	}

	// Get component
	component, err := components.Get(configManifest.ComponentKey())
	if err != nil {
		return err
	}

	// Component must be "scheduler"
	if !component.IsScheduler() {
		return nil
	}

	// Branch must be same
	if configKey.BranchKey() != configManifest.BranchKey() {
		panic(fmt.Errorf(`child "%s" and parent "%s" must be from same branch`, configManifest.String(), configKey.String()))
	}

	// Add relation
	configManifest.Relations.Add(&model.SchedulerForRelation{
		ComponentId: configKey.ComponentId,
		ConfigId:    configKey.Id,
	})

	return nil
}
