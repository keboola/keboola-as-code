package codes_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeLocalSave(t *testing.T) {
	t.Parallel()
	targetComponentId := storageapi.ComponentID(`keboola.python-transformation-v2`)

	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()
	_, rowState := createInternalSharedCode(t, targetComponentId, state)

	recipe := model.NewLocalSaveRecipe(rowState.Manifest(), rowState.Remote, model.NewChangedFields())
	codeFilePath := filesystem.Join(state.NamingGenerator().SharedCodeFilePath(recipe.ObjectManifest.Path(), targetComponentId))

	// Create dir
	assert.NoError(t, state.ObjectsRoot().Mkdir(filesystem.Dir(codeFilePath)))

	// Save to file
	assert.NoError(t, state.Mapper().MapBeforeLocalSave(context.Background(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Assert
	sharedCodeFileRaw := recipe.Files.GetOneByTag(model.FileKindNativeSharedCode)
	assert.NotNil(t, sharedCodeFileRaw)
	sharedCodeFile, err := sharedCodeFileRaw.ToRawFile()
	assert.NoError(t, err)
	assert.Equal(t, codeFilePath, sharedCodeFile.Path())
	assert.Equal(t, "foo\nbar\n", sharedCodeFile.Content)
}
