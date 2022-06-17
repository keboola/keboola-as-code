package codes_test

import (
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/codes"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func createStateWithMapper(t *testing.T) (*state.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyState()
	mockedState.Mapper().AddMapper(codes.NewMapper(mockedState))
	return mockedState, d
}

func createRemoteSharedCode(t *testing.T, state *state.State) (*model.ConfigState, *model.ConfigRowState) {
	t.Helper()
	targetComponentId := storageapi.ComponentID(`keboola.snowflake-transformation`)

	// Component
	state.Components().Set(&model.Component{
		ComponentKey: model.ComponentKey{
			Id: model.SharedCodeComponentId,
		},
		Type: `other`,
		Name: `Shared Code`,
	})

	// Target component
	state.Components().Set(&model.Component{
		ComponentKey: model.ComponentKey{
			Id: targetComponentId,
		},
		Type: `transformation`,
		Name: `Foo`,
	})

	// Config
	configKey := model.ConfigKey{
		BranchId:    789,
		Id:          `123`,
		ComponentId: model.SharedCodeComponentId,
	}
	configContent := orderedmap.New()
	configContent.Set(model.ShareCodeTargetComponentKey, targetComponentId.String())
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(
					"branch",
					"config",
				),
			},
		},
		Remote: &model.Config{
			ConfigKey: configKey,
			Content:   configContent,
		},
	}
	assert.NoError(t, state.Set(configState))

	// Row
	rowKey := model.ConfigRowKey{
		BranchId:    789,
		ConfigId:    `123`,
		Id:          `456`,
		ComponentId: model.SharedCodeComponentId,
	}
	rowState := &model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{
			ConfigRowKey: rowKey,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(
					"branch/config",
					"row",
				),
			},
		},
		Remote: &model.ConfigRow{
			ConfigRowKey: rowKey,
			Content:      orderedmap.New(),
		},
	}
	assert.NoError(t, state.Set(rowState))

	return configState, rowState
}

func createLocalSharedCode(t *testing.T, targetComponentId storageapi.ComponentID, state *state.State) (*model.ConfigState, *model.ConfigRowState) {
	t.Helper()

	// Component
	state.Components().Set(&model.Component{
		ComponentKey: model.ComponentKey{
			Id: model.SharedCodeComponentId,
		},
		Type: `other`,
		Name: `Shared Code`,
	})

	// Target component
	state.Components().Set(&model.Component{
		ComponentKey: model.ComponentKey{
			Id: targetComponentId,
		},
		Type: `transformation`,
		Name: `Foo`,
	})

	// Config
	configKey := model.ConfigKey{
		BranchId:    789,
		Id:          `123`,
		ComponentId: model.SharedCodeComponentId,
	}
	configContent := orderedmap.New()
	configContent.Set(model.ShareCodeTargetComponentKey, targetComponentId.String())
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(
					"branch",
					"config",
				),
			},
		},
		Local: &model.Config{
			ConfigKey: configKey,
			Content:   configContent,
		},
	}
	assert.NoError(t, state.Set(configState))

	// Row
	rowKey := model.ConfigRowKey{
		BranchId:    789,
		ConfigId:    `123`,
		Id:          `456`,
		ComponentId: model.SharedCodeComponentId,
	}
	rowState := &model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{
			ConfigRowKey: rowKey,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(
					"branch/config",
					"row",
				),
			},
		},
		Local: &model.ConfigRow{
			ConfigRowKey: rowKey,
			Content:      orderedmap.New(),
		},
	}
	assert.NoError(t, state.Set(rowState))

	return configState, rowState
}

// nolint: unparam
func createInternalSharedCode(t *testing.T, targetComponentId storageapi.ComponentID, state *state.State) (*model.ConfigState, *model.ConfigRowState) {
	t.Helper()

	// Component
	state.Components().Set(&model.Component{
		ComponentKey: model.ComponentKey{
			Id: model.SharedCodeComponentId,
		},
		Type: `other`,
		Name: `Shared Code`,
	})

	// Target component
	state.Components().Set(&model.Component{
		ComponentKey: model.ComponentKey{
			Id: targetComponentId,
		},
		Type: `transformation`,
		Name: `Foo`,
	})

	// Config
	configKey := model.ConfigKey{
		BranchId:    789,
		Id:          `123`,
		ComponentId: model.SharedCodeComponentId,
	}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(
					"branch",
					"config",
				),
			},
		},
		Local: &model.Config{
			ConfigKey: configKey,
			Content:   orderedmap.New(),
			SharedCode: &model.SharedCodeConfig{
				Target: targetComponentId,
			},
		},
		Remote: &model.Config{
			ConfigKey: configKey,
			Content:   orderedmap.New(),
			SharedCode: &model.SharedCodeConfig{
				Target: targetComponentId,
			},
		},
	}
	assert.NoError(t, state.Set(configState))

	// Row
	rowKey := model.ConfigRowKey{
		BranchId:    789,
		ConfigId:    `123`,
		Id:          `456`,
		ComponentId: model.SharedCodeComponentId,
	}
	rowState := &model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{
			ConfigRowKey: rowKey,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath(
					"branch/config",
					"row",
				),
			},
		},
		Local: &model.ConfigRow{
			ConfigRowKey: rowKey,
			Content:      orderedmap.New(),
			SharedCode: &model.SharedCodeRow{
				Target: targetComponentId,
				Scripts: model.Scripts{
					model.StaticScript{Value: `foo`},
					model.StaticScript{Value: `bar`},
				},
			},
		},
		Remote: &model.ConfigRow{
			ConfigRowKey: rowKey,
			Content:      orderedmap.New(),
			SharedCode: &model.SharedCodeRow{
				Target: targetComponentId,
				Scripts: model.Scripts{
					model.StaticScript{Value: `foo`},
					model.StaticScript{Value: `bar`},
				},
			},
		},
	}
	assert.NoError(t, state.Set(rowState))

	return configState, rowState
}
