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
	schedulerConfigKey, ok := recipe.Key.(model.ConfigKey)
	if !ok {
		return nil
	}

	// Parent of the scheduler must be target config
	parentConfigKey, ok := recipe.ParentKey.(model.ConfigKey)
	if !ok {
		return nil
	}

	// Get components
	components, err := m.Components()
	if err != nil {
		return err
	}

	// Get component
	component, err := components.Get(schedulerConfigKey.ComponentKey())
	if err != nil {
		return err
	}

	// Component must be "scheduler"
	if !component.IsScheduler() {
		return nil
	}

	// Branch must be same
	if parentConfigKey.BranchKey() != schedulerConfigKey.BranchKey() {
		panic(fmt.Errorf(`child "%s" and parent "%s" must be from same branch`, schedulerConfigKey.String(), parentConfigKey.String()))
	}

	// Add relation
	recipe.Relations.Add(&model.SchedulerForRelation{
		ComponentId: parentConfigKey.ComponentId,
		ConfigId:    parentConfigKey.ConfigId,
	})

	return nil
}
