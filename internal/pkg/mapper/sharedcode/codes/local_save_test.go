package codes_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/codes"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeLocalSave(t *testing.T) {
	t.Parallel()
	targetComponentId := model.ComponentId(`keboola.python-transformation-v2`)
	context, logs, _, rowState := createInternalSharedCode(t, targetComponentId)
	recipe := fixtures.NewLocalSaveRecipe(rowState.Manifest(), rowState.Remote)
	codeFilePath := filesystem.Join(context.NamingGenerator.SharedCodeFilePath(recipe.ObjectManifest.Path(), targetComponentId))

	// Create dir
	assert.NoError(t, context.Fs.Mkdir(filesystem.Dir(codeFilePath)))
	logs.Truncate()

	// Save to file
	assert.NoError(t, NewMapper(context).MapBeforeLocalSave(recipe))
	assert.Empty(t, logs.String())

	// Assert
	sharedCodeFileRaw := recipe.Files.GetOneByTag(model.FileKindNativeSharedCode)
	assert.NotNil(t, sharedCodeFileRaw)
	sharedCodeFile, err := sharedCodeFileRaw.ToFile()
	assert.NoError(t, err)
	assert.Equal(t, codeFilePath, sharedCodeFile.Path)
	assert.Equal(t, "foo\nbar\n", sharedCodeFile.Content)
}
