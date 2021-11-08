package sharedcode_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// nolint: unparam
func createTestFixtures(t *testing.T, targetComponentId string) (model.MapperContext, *model.ConfigRow, *model.ConfigRowManifest) {
	t.Helper()

	logger, _ := utils.NewDebugLogger()
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
	configRecord := &model.ConfigManifest{
		ConfigKey: configKey,
		Paths: model.Paths{
			PathInProject: model.NewPathInProject(
				"branch",
				"config",
			),
		},
	}
	config := &model.Config{
		ConfigKey: configKey,
		Content:   utils.NewOrderedMap(),
	}
	config.Content.Set(model.ShareCodeTargetComponentKey, targetComponentId)
	configState, err := state.CreateFrom(configRecord)
	assert.NoError(t, err)
	configState.SetLocalState(config)
	configState.SetRemoteState(config)

	// Row
	rowKey := model.ConfigRowKey{
		BranchId:    789,
		ConfigId:    `123`,
		Id:          `456`,
		ComponentId: model.SharedCodeComponentId,
	}
	rowRecord := &model.ConfigRowManifest{
		ConfigRowKey: rowKey,
		Paths: model.Paths{
			PathInProject: model.NewPathInProject(
				"branch/config",
				"row",
			),
		},
	}
	row := &model.ConfigRow{
		ConfigRowKey: rowKey,
		Content:      utils.NewOrderedMap(),
	}
	rowState, err := state.GetOrCreateFrom(rowRecord)
	assert.NoError(t, err)
	rowState.SetLocalState(row)
	rowState.SetRemoteState(row)

	context := model.MapperContext{Logger: logger, Fs: fs, Naming: model.DefaultNaming(), State: state}
	return context, row, rowRecord
}

func createLocalLoadRecipe(row *model.ConfigRow, rowRecord *model.ConfigRowManifest) *model.LocalLoadRecipe {
	return &model.LocalLoadRecipe{
		Object:        row,
		Record:        rowRecord,
		Metadata:      filesystem.CreateJsonFile(model.MetaFile, utils.NewOrderedMap()),
		Configuration: filesystem.CreateJsonFile(model.ConfigFile, utils.NewOrderedMap()),
		Description:   filesystem.CreateFile(model.DescriptionFile, ``),
	}
}

func createLocalSaveRecipe(row *model.ConfigRow, rowRecord *model.ConfigRowManifest) *model.LocalSaveRecipe {
	return &model.LocalSaveRecipe{
		Object:        row,
		Record:        rowRecord,
		Metadata:      filesystem.CreateJsonFile(model.MetaFile, utils.NewOrderedMap()),
		Configuration: filesystem.CreateJsonFile(model.ConfigFile, utils.NewOrderedMap()),
		Description:   filesystem.CreateFile(model.DescriptionFile, ``),
	}
}

func createMapperContext(t *testing.T) model.MapperContext {
	t.Helper()
	logger, _ := utils.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, ".")
	assert.NoError(t, err)
	state := model.NewState(zap.NewNop().Sugar(), fs, model.NewComponentsMap(testapi.NewMockedComponentsProvider()), model.SortByPath)
	return model.MapperContext{Logger: logger, Fs: fs, Naming: model.DefaultNaming(), State: state}
}
