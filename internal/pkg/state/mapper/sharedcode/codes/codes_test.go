package codes_test

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state/mapper/sharedcode/codes"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func createStateWithLocalMapper(t *testing.T) (*local.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyLocalState()
	mockedState.Mapper().AddMapper(codes.NewLocalMapper(mockedState, d))
	return mockedState, d
}

func createStateWithRemoteMapper(t *testing.T) (*remote.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyRemoteState()
	mockedState.Mapper().AddMapper(codes.NewRemoteMapper(mockedState, d))
	return mockedState, d
}

func createSharedCode(t *testing.T, targetComponentId model.ComponentId, state model.Objects, addToState bool) (*model.Config, *model.ConfigRow) {
	t.Helper()

	// Branch
	state.MustAdd(&model.Branch{
		BranchKey: model.BranchKey{BranchId: 123},
	})

	// Config
	configKey := model.ConfigKey{
		BranchId:    123,
		ConfigId:    `123`,
		ComponentId: model.SharedCodeComponentId,
	}
	configContent := orderedmap.New()
	configContent.Set(model.ShareCodeTargetComponentKey, targetComponentId.String())
	config := &model.Config{
		ConfigKey: configKey,
		Content:   configContent,
	}

	// Row
	rowKey := model.ConfigRowKey{
		BranchId:    123,
		ConfigId:    `123`,
		ConfigRowId: `456`,
		ComponentId: model.SharedCodeComponentId,
	}
	row := &model.ConfigRow{
		ConfigRowKey: rowKey,
		Content:      orderedmap.New(),
	}

	// False for testing MapAfterLocalLoad - objects have not yet been added to the state
	// True for testing AfterRemoteOperation -the objects are in the state
	if addToState {
		state.MustAdd(config, row)
	}

	return config, row
}

// nolint: unparam
func createInternalSharedCode(t *testing.T, targetComponentId model.ComponentId, state model.Objects) (*model.Config, *model.ConfigRow) {
	t.Helper()

	// Branch
	state.MustAdd(&model.Branch{
		BranchKey: model.BranchKey{BranchId: 123},
	})

	// Config
	configKey := model.ConfigKey{
		BranchId:    123,
		ConfigId:    `123`,
		ComponentId: model.SharedCodeComponentId,
	}
	config := &model.Config{
		ConfigKey: configKey,
		Content:   orderedmap.New(),
		SharedCode: &model.SharedCodeConfig{
			Target: targetComponentId,
		},
	}
	state.MustAdd(config)

	// Row
	rowKey := model.ConfigRowKey{
		BranchId:    123,
		ConfigId:    `123`,
		ConfigRowId: `456`,
		ComponentId: model.SharedCodeComponentId,
	}
	row := &model.ConfigRow{
		ConfigRowKey: rowKey,
		Content:      orderedmap.New(),
		SharedCode: &model.SharedCodeRow{
			Target: targetComponentId,
			Scripts: model.Scripts{
				model.StaticScript{Value: `foo`},
				model.StaticScript{Value: `bar`},
			},
		},
	}
	state.MustAdd(row)

	return config, row
}
