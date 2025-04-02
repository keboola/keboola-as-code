package codes_test

import (
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeLocalSave(t *testing.T) {
	t.Parallel()
	targetComponentID := keboola.ComponentID(`keboola.python-transformation-v2`)

	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()
	_, rowState := createInternalSharedCode(t, targetComponentID, state)

	recipe := model.NewLocalSaveRecipe(rowState.Manifest(), rowState.Remote, model.NewChangedFields())
	codeFilePath := filesystem.Join(state.NamingGenerator().SharedCodeFilePath(recipe.Path(), targetComponentID))

	// Create dir
	require.NoError(t, state.ObjectsRoot().Mkdir(t.Context(), filesystem.Dir(codeFilePath)))

	// Save to file
	require.NoError(t, state.Mapper().MapBeforeLocalSave(t.Context(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Assert
	sharedCodeFileRaw := recipe.Files.GetOneByTag(model.FileKindNativeSharedCode)
	assert.NotNil(t, sharedCodeFileRaw)
	sharedCodeFile, err := sharedCodeFileRaw.ToRawFile()
	require.NoError(t, err)
	assert.Equal(t, codeFilePath, sharedCodeFile.Path())
	assert.Equal(t, "foo\nbar\n", sharedCodeFile.Content)
}
