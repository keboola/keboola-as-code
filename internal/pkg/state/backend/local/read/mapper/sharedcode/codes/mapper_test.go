package codes_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/read/mapper/sharedcode/codes"
)

func TestSharedCodeLocalLoad(t *testing.T) {
	t.Parallel()
	targetComponentId := storageapi.ComponentID(`keboola.python-transformation-v2`)

	s, d := createStateWithMapper(t)
	logger := d.DebugLogger()
	fs := s.ObjectsRoot()
	configState, rowState := createSharedCode(t, targetComponentId, s)

	// Write file
	codeFilePath := filesystem.Join(s.NamingGenerator().SharedCodeFilePath(rowState.ConfigRowManifest.Path(), targetComponentId))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(codeFilePath, `foo bar`)))
	logger.Truncate()

	// Load config
	configRecipe := model.NewLocalLoadRecipe(s.FileLoader(), configState.Manifest(), configState.Local)
	err := s.Mapper().MapAfterLocalLoad(context.Background(), configRecipe)
	assert.NoError(t, err)
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Load row
	rowRecipe := model.NewLocalLoadRecipe(s.FileLoader(), rowState.Manifest(), rowState.Local)
	err = s.Mapper().MapAfterLocalLoad(context.Background(), rowRecipe)
	assert.NoError(t, err)
	assert.Equal(t, "DEBUG  Loaded \"branch/config/row/code.py\"\n", logger.AllMessages())

	// Structs are set
	assert.Equal(t, &model.SharedCodeConfig{
		Target: "keboola.python-transformation-v2",
	}, configState.Local.SharedCode)
	assert.Equal(t, &model.SharedCodeRow{
		Target: "keboola.python-transformation-v2",
		Scripts: model.Scripts{
			model.StaticScript{Value: `foo bar`},
		},
	}, rowState.Local.SharedCode)

	// Shared code is loaded
	sharedCodeFile := rowRecipe.Files.GetOneByTag(model.FileKindNativeSharedCode)
	assert.NotNil(t, sharedCodeFile)
}

func TestSharedCodeLocalLoad_MissingCodeFile(t *testing.T) {
	t.Parallel()
	targetComponentId := storageapi.ComponentID(`keboola.python-transformation-v2`)

	s, d := createStateWithMapper(t)
	logger := d.DebugLogger()
	configState, rowState := createSharedCode(t, targetComponentId, s)

	// Load config
	configRecipe := model.NewLocalLoadRecipe(s.FileLoader(), configState.Manifest(), configState.Local)
	err := s.Mapper().MapAfterLocalLoad(context.Background(), configRecipe)
	assert.NoError(t, err)
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Load row
	rowRecipe := model.NewLocalLoadRecipe(s.FileLoader(), rowState.Manifest(), rowState.Local)
	err = s.Mapper().MapAfterLocalLoad(context.Background(), rowRecipe)
	assert.Error(t, err)
	assert.Equal(t, `missing shared code file "branch/config/row/code.py"`, err.Error())
	assert.Empty(t, logger.WarnAndErrorMessages())
}

func createSharedCode(t *testing.T, targetComponentId storageapi.ComponentID, state *state.State) (*model.ConfigState, *model.ConfigRowState) {
	t.Helper()

	// Config
	configKey := model.ConfigKey{
		BranchId:    789,
		Id:          `123`,
		ComponentId: storageapi.SharedCodeComponentID,
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
		ComponentId: storageapi.SharedCodeComponentID,
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

func createStateWithMapper(t *testing.T) (*state.State, dependencies.Mocked) {
	t.Helper()
	d := dependencies.NewMockedDeps()
	mockedState := d.MockedState()
	mockedState.Mapper().AddMapper(codes.NewMapper(mockedState))
	return mockedState, d
}
