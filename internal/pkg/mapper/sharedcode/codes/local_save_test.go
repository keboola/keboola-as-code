package codes_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/codes"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeSaveMissingKey(t *testing.T) {
	t.Parallel()
	targetComponentId := model.ComponentId(`keboola.python-transformation-v2`)
	context, rowState := createTestFixtures(t, targetComponentId)
	recipe := fixtures.NewLocalSaveRecipe(rowState.Manifest(), rowState.Local)

	err := NewMapper(context).MapBeforeLocalSave(recipe)
	assert.Error(t, err)
	assert.Equal(t, `key "code_content" not found in config row "branch:789/component:keboola.shared-code/config:123/row:456"`, err.Error())
	assert.Nil(t, recipe.Files.GetOneByTag(model.FileKindNativeSharedCode))
}

func TestSharedCodeSave(t *testing.T) {
	t.Parallel()
	targetComponentId := model.ComponentId(`keboola.python-transformation-v2`)
	context, rowState := createTestFixtures(t, targetComponentId)
	recipe := fixtures.NewLocalSaveRecipe(rowState.Manifest(), rowState.Local)
	codeFilePath := filesystem.Join(context.Naming.SharedCodeFilePath(recipe.ObjectManifest.Path(), targetComponentId))

	// Set JSON value
	rowState.Local.Content.Set(model.SharedCodeContentKey, []interface{}{`foo`, `bar`})

	// Create dir
	assert.NoError(t, context.Fs.Mkdir(filesystem.Dir(codeFilePath)))

	// Save to file
	err := NewMapper(context).MapBeforeLocalSave(recipe)

	// Assert
	assert.NoError(t, err)
	sharedCodeFileRaw := recipe.Files.GetOneByTag(model.FileKindNativeSharedCode)
	assert.NotNil(t, sharedCodeFileRaw)
	sharedCodeFile, err := sharedCodeFileRaw.ToFile()
	assert.NoError(t, err)
	assert.Equal(t, codeFilePath, sharedCodeFile.Path)
	assert.Equal(t, "foo\nbar\n", sharedCodeFile.Content)
}
