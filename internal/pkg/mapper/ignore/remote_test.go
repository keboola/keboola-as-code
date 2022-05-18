package ignore_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestIgnoreMapper_AfterRemoteOperation_Variables(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Target1
	targetKey := model.ConfigKey{BranchId: 1, ComponentId: "keboola.foo-bar", Id: "1"}
	targetConfig := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: targetKey},
		Remote:         &model.Config{ConfigKey: targetKey},
	}
	assert.NoError(t, state.Set(targetConfig))

	// Variables for target
	targetVarsKey := model.ConfigKey{BranchId: 1, ComponentId: model.VariablesComponentId, Id: "2"}
	targetVars := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: targetVarsKey},
		Remote: &model.Config{
			ConfigKey: targetVarsKey,
			Relations: model.Relations{
				&model.VariablesForRelation{
					ComponentId: targetKey.ComponentId,
					ConfigId:    targetKey.Id,
				},
			},
		},
	}
	assert.NoError(t, state.Set(targetVars))

	// Unattached variables
	unattachedVarsKey := model.ConfigKey{BranchId: 1, ComponentId: model.VariablesComponentId, Id: "3"}
	unattachedVars := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: unattachedVarsKey},
		Remote:         &model.Config{ConfigKey: unattachedVarsKey},
	}
	assert.NoError(t, state.Set(unattachedVars))

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddLoaded(targetConfig)
	changes.AddLoaded(targetVars)
	changes.AddLoaded(unattachedVars)
	assert.NoError(t, state.Mapper().AfterRemoteOperation(changes))
	assert.Equal(t, "DEBUG  Ignored unattached variables config \"branch:1/component:keboola.variables/config:3\"\n", logger.AllMessages())

	// Unattached variables are removed
	assert.Equal(t, []model.ObjectState{
		targetConfig,
		targetVars,
	}, state.All())
}

func TestIgnoreMapper_AfterRemoteOperation_Scheduler(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Target for valid scheduler
	targetKey := model.ConfigKey{BranchId: 1, ComponentId: "keboola.foo-bar", Id: "1"}
	targetConfig := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: targetKey},
		Remote:         &model.Config{ConfigKey: targetKey},
	}
	assert.NoError(t, state.Set(targetConfig))

	// Valid scheduler
	validSchedulerKey := model.ConfigKey{BranchId: 1, ComponentId: model.SchedulerComponentId, Id: "2"}
	validScheduler := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: validSchedulerKey},
		Remote: &model.Config{
			ConfigKey: validSchedulerKey,
			Relations: model.Relations{
				&model.SchedulerForRelation{
					ComponentId: targetKey.ComponentId,
					ConfigId:    targetKey.Id,
				},
			},
		},
	}
	assert.NoError(t, state.Set(validScheduler))

	// Ignored scheduler
	missingTargetKey := model.ConfigKey{BranchId: 1, ComponentId: "keboola.foo-bar", Id: "789"}
	ignoredSchedulerKey := model.ConfigKey{BranchId: 1, ComponentId: model.SchedulerComponentId, Id: "3"}
	ignoredScheduler := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: ignoredSchedulerKey},
		Remote: &model.Config{
			ConfigKey: ignoredSchedulerKey,
			Relations: model.Relations{
				&model.SchedulerForRelation{
					ComponentId: missingTargetKey.ComponentId,
					ConfigId:    missingTargetKey.Id,
				},
			},
		},
	}
	assert.NoError(t, state.Set(ignoredScheduler))

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddLoaded(targetConfig)
	changes.AddLoaded(ignoredScheduler)
	changes.AddLoaded(validScheduler)
	assert.NoError(t, state.Mapper().AfterRemoteOperation(changes))
	assert.Equal(t, "DEBUG  Ignored scheduler config \"branch:1/component:keboola.scheduler/config:3\", target config \"branch:1/component:keboola.foo-bar/config:789\" not found\n", logger.AllMessages())

	// Unattached variables are removed
	assert.Equal(t, []model.ObjectState{
		targetConfig,
		validScheduler,
	}, state.All())
}
