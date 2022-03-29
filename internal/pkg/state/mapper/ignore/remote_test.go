package ignore_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestIgnoreMapper_AfterRemoteOperation(t *testing.T) {
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
