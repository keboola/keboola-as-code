package codes_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/codes"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func createStateWithMapper(t *testing.T) (*state.State, dependencies.Mocked) {
	t.Helper()
	d := dependencies.NewMocked(t, context.Background())
	mockedState := d.MockedState()
	mockedState.Mapper().AddMapper(codes.NewMapper(mockedState))
	return mockedState, d
}

func createRemoteSharedCode(t *testing.T, state *state.State) (*model.ConfigState, *model.ConfigRowState) {
	t.Helper()
	targetComponentID := keboola.ComponentID(`keboola.snowflake-transformation`)

	// Config
	configKey := model.ConfigKey{
		BranchID:    789,
		ID:          `123`,
		ComponentID: keboola.SharedCodeComponentID,
	}
	configContent := orderedmap.New()
	configContent.Set(model.ShareCodeTargetComponentKey, targetComponentID.String())
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
	require.NoError(t, state.Set(configState))

	// Row
	rowKey := model.ConfigRowKey{
		BranchID:    789,
		ConfigID:    `123`,
		ID:          `456`,
		ComponentID: keboola.SharedCodeComponentID,
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
	require.NoError(t, state.Set(rowState))

	return configState, rowState
}

func createLocalSharedCode(t *testing.T, targetComponentID keboola.ComponentID, state *state.State) (*model.ConfigState, *model.ConfigRowState) {
	t.Helper()

	// Config
	configKey := model.ConfigKey{
		BranchID:    789,
		ID:          `123`,
		ComponentID: keboola.SharedCodeComponentID,
	}
	configContent := orderedmap.New()
	configContent.Set(model.ShareCodeTargetComponentKey, targetComponentID.String())
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
	require.NoError(t, state.Set(configState))

	// Row
	rowKey := model.ConfigRowKey{
		BranchID:    789,
		ConfigID:    `123`,
		ID:          `456`,
		ComponentID: keboola.SharedCodeComponentID,
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
	require.NoError(t, state.Set(rowState))

	return configState, rowState
}

// nolint: unparam
func createInternalSharedCode(t *testing.T, targetComponentID keboola.ComponentID, state *state.State) (*model.ConfigState, *model.ConfigRowState) {
	t.Helper()

	// Config
	configKey := model.ConfigKey{
		BranchID:    789,
		ID:          `123`,
		ComponentID: keboola.SharedCodeComponentID,
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
				Target: targetComponentID,
			},
		},
		Remote: &model.Config{
			ConfigKey: configKey,
			Content:   orderedmap.New(),
			SharedCode: &model.SharedCodeConfig{
				Target: targetComponentID,
			},
		},
	}
	require.NoError(t, state.Set(configState))

	// Row
	rowKey := model.ConfigRowKey{
		BranchID:    789,
		ConfigID:    `123`,
		ID:          `456`,
		ComponentID: keboola.SharedCodeComponentID,
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
				Target: targetComponentID,
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
				Target: targetComponentID,
				Scripts: model.Scripts{
					model.StaticScript{Value: `foo`},
					model.StaticScript{Value: `bar`},
				},
			},
		},
	}
	require.NoError(t, state.Set(rowState))

	return configState, rowState
}
