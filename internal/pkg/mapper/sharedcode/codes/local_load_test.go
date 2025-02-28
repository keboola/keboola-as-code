package codes_test

import (
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeLocalLoad(t *testing.T) {
	t.Parallel()
	targetComponentID := keboola.ComponentID(`keboola.python-transformation-v2`)

	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()
	fs := state.ObjectsRoot()
	configState, rowState := createLocalSharedCode(t, targetComponentID, state)

	// Write file
	codeFilePath := filesystem.Join(state.NamingGenerator().SharedCodeFilePath(rowState.ConfigRowManifest.Path(), targetComponentID))
	require.NoError(t, fs.WriteFile(t.Context(), filesystem.NewRawFile(codeFilePath, `foo bar`)))
	logger.Truncate()

	// Load config
	configRecipe := model.NewLocalLoadRecipe(state.FileLoader(), configState.Manifest(), configState.Local)
	err := state.Mapper().MapAfterLocalLoad(t.Context(), configRecipe)
	require.NoError(t, err)
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Load row
	rowRecipe := model.NewLocalLoadRecipe(state.FileLoader(), rowState.Manifest(), rowState.Local)
	err = state.Mapper().MapAfterLocalLoad(t.Context(), rowRecipe)
	require.NoError(t, err)
	logger.AssertJSONMessages(t, `{"level":"debug","message":"Loaded \"branch/config/row/code.py\""}`)

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
	targetComponentID := keboola.ComponentID(`keboola.python-transformation-v2`)

	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()
	configState, rowState := createLocalSharedCode(t, targetComponentID, state)

	// Load config
	configRecipe := model.NewLocalLoadRecipe(state.FileLoader(), configState.Manifest(), configState.Local)
	err := state.Mapper().MapAfterLocalLoad(t.Context(), configRecipe)
	require.NoError(t, err)
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Load row
	rowRecipe := model.NewLocalLoadRecipe(state.FileLoader(), rowState.Manifest(), rowState.Local)
	err = state.Mapper().MapAfterLocalLoad(t.Context(), rowRecipe)
	require.Error(t, err)
	assert.Equal(t, `missing shared code file "branch/config/row/code.py"`, err.Error())
	assert.Empty(t, logger.WarnAndErrorMessages())
}
