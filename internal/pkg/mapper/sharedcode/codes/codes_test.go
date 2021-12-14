package codes_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func createRemoteSharedCode(t *testing.T) (model.MapperContext, *utils.Writer, *model.ConfigState, *model.ConfigRowState) {
	t.Helper()

	targetComponentId := model.ComponentId(`keboola.snowflake-transformation`)
	logger, logs := utils.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, ".")
	assert.NoError(t, err)

	state := model.NewState(zap.NewNop().Sugar(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)

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
				PathInProject: model.NewPathInProject(
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
				PathInProject: model.NewPathInProject(
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

	context := model.MapperContext{Logger: logger, Fs: fs, Naming: model.DefaultNamingWithIds(), State: state}
	return context, logs, configState, rowState
}

func createLocalSharedCode(t *testing.T, targetComponentId model.ComponentId) (model.MapperContext, *utils.Writer, *model.ConfigState, *model.ConfigRowState) {
	t.Helper()

	logger, logs := utils.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, ".")
	assert.NoError(t, err)

	state := model.NewState(zap.NewNop().Sugar(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)

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
				PathInProject: model.NewPathInProject(
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
				PathInProject: model.NewPathInProject(
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

	context := model.MapperContext{Logger: logger, Fs: fs, Naming: model.DefaultNamingWithIds(), State: state}
	return context, logs, configState, rowState
}

// nolint: unparam
func createInternalSharedCode(t *testing.T, targetComponentId model.ComponentId) (model.MapperContext, *utils.Writer, *model.ConfigState, *model.ConfigRowState) {
	t.Helper()

	logger, logs := utils.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, ".")
	assert.NoError(t, err)

	state := model.NewState(zap.NewNop().Sugar(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)

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
				PathInProject: model.NewPathInProject(
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
				PathInProject: model.NewPathInProject(
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

	context := model.MapperContext{Logger: logger, Fs: fs, Naming: model.DefaultNamingWithIds(), State: state}
	return context, logs, configState, rowState
}
