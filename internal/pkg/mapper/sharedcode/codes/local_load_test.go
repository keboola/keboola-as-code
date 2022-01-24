package codes_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeLocalLoad(t *testing.T) {
	t.Parallel()
	targetComponentId := model.ComponentId(`keboola.python-transformation-v2`)

	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()
	fs := d.Fs()
	configState, rowState := createLocalSharedCode(t, targetComponentId, state)

	// Write file
	codeFilePath := filesystem.Join(state.NamingGenerator().SharedCodeFilePath(rowState.ConfigRowManifest.Path(), targetComponentId))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(codeFilePath, `foo bar`)))
	logger.Truncate()

	// Load config
	configRecipe := model.NewLocalLoadRecipe(d.FileLoader(), configState.Manifest(), configState.Local)
	err := state.Mapper().MapAfterLocalLoad(configRecipe)
	assert.NoError(t, err)
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Load row
	rowRecipe := model.NewLocalLoadRecipe(d.FileLoader(), rowState.Manifest(), rowState.Local)
	err = state.Mapper().MapAfterLocalLoad(rowRecipe)
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
	targetComponentId := model.ComponentId(`keboola.python-transformation-v2`)

	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()
	configState, rowState := createLocalSharedCode(t, targetComponentId, state)

	// Load config
	configRecipe := model.NewLocalLoadRecipe(d.FileLoader(), configState.Manifest(), configState.Local)
	err := state.Mapper().MapAfterLocalLoad(configRecipe)
	assert.NoError(t, err)
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Load row
	rowRecipe := model.NewLocalLoadRecipe(d.FileLoader(), rowState.Manifest(), rowState.Local)
	err = state.Mapper().MapAfterLocalLoad(rowRecipe)
	assert.Error(t, err)
	assert.Equal(t, `missing shared code file "branch/config/row/code.py"`, err.Error())
	assert.Empty(t, logger.WarnAndErrorMessages())
}
