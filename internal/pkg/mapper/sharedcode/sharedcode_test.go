package sharedcode

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// nolint: unparam
func createTestFixtures(t *testing.T, targetComponentId string) (*zap.SugaredLogger, filesystem.Fs, *model.State, *model.ConfigRow, *model.ObjectFiles) {
	t.Helper()

	logger, _ := utils.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, ".")
	assert.NoError(t, err)

	state := model.NewState(zap.NewNop().Sugar(), fs, model.NewComponentsMap(nil), model.SortByPath)

	// Component
	state.Components().Set(&model.Component{
		ComponentKey: model.ComponentKey{
			Id: model.ShareCodeComponentId,
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
		ComponentId: model.ShareCodeComponentId,
	}
	configRecord := &model.ConfigManifest{
		ConfigKey: configKey,
		Paths: model.Paths{
			PathInProject: model.PathInProject{
				ParentPath: "branch",
				ObjectPath: "config",
			},
		},
	}
	config := &model.Config{
		ConfigKey: configKey,
		Content:   utils.NewOrderedMap(),
	}
	config.Content.Set(model.ShareCodeTargetComponentKey, targetComponentId)
	configStateRaw, err := state.GetOrCreate(configKey)
	assert.NoError(t, err)
	configState := configStateRaw.(*model.ConfigState)
	configState.SetManifest(configRecord)
	configState.SetLocalState(config)
	configState.SetRemoteState(config)

	// Row
	rowKey := model.ConfigRowKey{
		BranchId:    789,
		ConfigId:    `123`,
		Id:          `456`,
		ComponentId: model.ShareCodeComponentId,
	}
	rowRecord := &model.ConfigRowManifest{
		ConfigRowKey: rowKey,
		Paths: model.Paths{
			PathInProject: model.PathInProject{
				ParentPath: "branch/config",
				ObjectPath: "row",
			},
		},
	}
	row := &model.ConfigRow{
		ConfigRowKey: rowKey,
		Content:      utils.NewOrderedMap(),
	}
	rowStateRaw, err := state.GetOrCreate(rowKey)
	assert.NoError(t, err)
	rowState := rowStateRaw.(*model.ConfigRowState)
	rowState.SetManifest(rowRecord)
	rowState.SetLocalState(row)
	rowState.SetRemoteState(row)

	// Files
	objectFiles := &model.ObjectFiles{
		Object:        row,
		Record:        rowRecord,
		Metadata:      filesystem.CreateJsonFile(model.MetaFile, utils.NewOrderedMap()),
		Configuration: filesystem.CreateJsonFile(model.ConfigFile, utils.NewOrderedMap()),
		Description:   filesystem.CreateFile(model.DescriptionFile, ``),
	}

	return logger, fs, state, row, objectFiles
}
