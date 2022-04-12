package ignoreobject_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state/mapper/ignoreobject"
)

func TestIgnoreMapper_AfterRemoteOperation(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Branch
	branchKey := model.BranchKey{BranchId: 1}
	branch := &model.Branch{BranchKey: branchKey}
	state.MustAdd(branch)

	// Target1
	configKey := model.ConfigKey{BranchKey: branchKey, ComponentId: "keboola.foo-bar", ConfigId: "1"}
	config := &model.Config{ConfigKey: configKey}
	state.MustAdd(config)

	// Variables for target
	attachedVariables := &model.Config{
		ConfigKey: model.ConfigKey{BranchKey: branchKey, ComponentId: model.VariablesComponentId, ConfigId: "2"},
		Relations: model.Relations{
			&model.VariablesForRelation{
				ComponentId: configKey.ComponentId,
				ConfigId:    configKey.ConfigId,
			},
		},
	}
	state.MustAdd(attachedVariables)

	// Unattached variables
	unattachedVariables := &model.Config{ConfigKey: model.ConfigKey{BranchKey: branchKey, ComponentId: model.VariablesComponentId, ConfigId: "3"}}
	state.MustAdd(unattachedVariables)

	// Invoke
	assert.NoError(t, state.Mapper().AfterRemoteOperation(model.NewChanges().AddLoaded(config, attachedVariables, unattachedVariables)))
	assert.Equal(t, "DEBUG  Ignored unattached variables config \"branch:1/component:keboola.variables/config:3\"\n", logger.AllMessages())

	// Unattached variables are removed
	assert.Equal(t, []model.Object{
		branch,
		config,
		attachedVariables,
	}, state.All())
}

func createStateWithMapper(t *testing.T) (*remote.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyRemoteState()
	mockedState.Mapper().AddMapper(ignoreobject.NewRemoteMapper(mockedState, d))
	return mockedState, d
}
