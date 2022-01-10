package codes_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeLocalSave(t *testing.T) {
	t.Parallel()
	targetComponentId := model.ComponentId(`keboola.python-transformation-v2`)

	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()
	fs := d.Fs()
	_, rowState := createInternalSharedCode(t, targetComponentId, state)

	recipe := fixtures.NewLocalSaveRecipe(rowState.Manifest(), rowState.Remote)
	codeFilePath := filesystem.Join(state.NamingGenerator().SharedCodeFilePath(recipe.ObjectManifest.Path(), targetComponentId))

	// Create dir
	assert.NoError(t, fs.Mkdir(filesystem.Dir(codeFilePath)))

	// Save to file
	assert.NoError(t, state.Mapper().MapBeforeLocalSave(recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Assert
	sharedCodeFileRaw := recipe.Files.GetOneByTag(model.FileKindNativeSharedCode)
	assert.NotNil(t, sharedCodeFileRaw)
	sharedCodeFile, err := sharedCodeFileRaw.ToFile()
	assert.NoError(t, err)
	assert.Equal(t, codeFilePath, sharedCodeFile.Path)
	assert.Equal(t, "foo\nbar\n", sharedCodeFile.Content)
}
