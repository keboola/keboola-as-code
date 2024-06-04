package ignore_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestIgnoreMapper_AfterRemoteOperation_Variables(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Target1
	targetKey := model.ConfigKey{BranchID: 1, ComponentID: "keboola.foo-bar", ID: "1"}
	targetConfig := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: targetKey},
		Remote:         &model.Config{ConfigKey: targetKey},
	}
	require.NoError(t, state.Set(targetConfig))

	// Variables for target
	targetVarsKey := model.ConfigKey{BranchID: 1, ComponentID: keboola.VariablesComponentID, ID: "2"}
	targetVars := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: targetVarsKey},
		Remote: &model.Config{
			ConfigKey: targetVarsKey,
			Relations: model.Relations{
				&model.VariablesForRelation{
					ComponentID: targetKey.ComponentID,
					ConfigID:    targetKey.ID,
				},
			},
		},
	}
	require.NoError(t, state.Set(targetVars))

	// Unattached variables
	unattachedVarsKey := model.ConfigKey{BranchID: 1, ComponentID: keboola.VariablesComponentID, ID: "3"}
	unattachedVars := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: unattachedVarsKey},
		Remote:         &model.Config{ConfigKey: unattachedVarsKey},
	}
	require.NoError(t, state.Set(unattachedVars))

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddLoaded(targetConfig)
	changes.AddLoaded(targetVars)
	changes.AddLoaded(unattachedVars)
	require.NoError(t, state.Mapper().AfterRemoteOperation(context.Background(), changes))
	logger.AssertJSONMessages(t, `{"level":"debug","message":"Ignored unattached variables config \"branch:1/component:keboola.variables/config:3\""}`)

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
	targetKey := model.ConfigKey{BranchID: 1, ComponentID: "keboola.foo-bar", ID: "1"}
	targetConfig := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: targetKey},
		Remote:         &model.Config{ConfigKey: targetKey},
	}
	require.NoError(t, state.Set(targetConfig))

	// Valid scheduler
	validSchedulerKey := model.ConfigKey{BranchID: 1, ComponentID: keboola.SchedulerComponentID, ID: "2"}
	validScheduler := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: validSchedulerKey},
		Remote: &model.Config{
			ConfigKey: validSchedulerKey,
			Relations: model.Relations{
				&model.SchedulerForRelation{
					ComponentID: targetKey.ComponentID,
					ConfigID:    targetKey.ID,
				},
			},
		},
	}
	require.NoError(t, state.Set(validScheduler))

	// Ignored scheduler
	missingTargetKey := model.ConfigKey{BranchID: 1, ComponentID: "keboola.foo-bar", ID: "789"}
	ignoredSchedulerKey := model.ConfigKey{BranchID: 1, ComponentID: keboola.SchedulerComponentID, ID: "3"}
	ignoredScheduler := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: ignoredSchedulerKey},
		Remote: &model.Config{
			ConfigKey: ignoredSchedulerKey,
			Relations: model.Relations{
				&model.SchedulerForRelation{
					ComponentID: missingTargetKey.ComponentID,
					ConfigID:    missingTargetKey.ID,
				},
			},
		},
	}
	require.NoError(t, state.Set(ignoredScheduler))

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddLoaded(targetConfig)
	changes.AddLoaded(ignoredScheduler)
	changes.AddLoaded(validScheduler)
	require.NoError(t, state.Mapper().AfterRemoteOperation(context.Background(), changes))
	logger.AssertJSONMessages(t, `{"level":"debug","message":"Ignored scheduler config \"branch:1/component:keboola.scheduler/config:3\", target config \"branch:1/component:keboola.foo-bar/config:789\" not found"}`)

	// Unattached variables are removed
	assert.Equal(t, []model.ObjectState{
		targetConfig,
		validScheduler,
	}, state.All())
}
