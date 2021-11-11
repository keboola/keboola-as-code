package codes_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/codes"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeSaveMissingKey(t *testing.T) {
	t.Parallel()
	targetComponentId := `keboola.python-transformation-v2`
	context, row, rowRecord := createTestFixtures(t, targetComponentId)
	recipe := createLocalSaveRecipe(row, rowRecord)

	err := NewMapper(context).MapBeforeLocalSave(recipe)
	assert.Error(t, err)
	assert.Equal(t, `key "code_content" not found in config row "branch:789/component:keboola.shared-code/config:123/row:456"`, err.Error())
	assert.Len(t, recipe.ExtraFiles, 0)
}

func TestSharedCodeSaveString(t *testing.T) {
	t.Parallel()
	targetComponentId := `keboola.python-transformation-v2`
	context, row, rowRecord := createTestFixtures(t, targetComponentId)
	recipe := createLocalSaveRecipe(row, rowRecord)
	codeFilePath := filesystem.Join(context.Naming.SharedCodeFilePath(recipe.Record.Path(), targetComponentId))

	// Set JSON value
	row.Content.Set(model.ShareCodeContentKey, `foo bar`)

	// Create dir
	assert.NoError(t, context.Fs.Mkdir(filesystem.Dir(codeFilePath)))

	// Save to file
	err := NewMapper(context).MapBeforeLocalSave(recipe)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, recipe.ExtraFiles, 1)
	file := recipe.ExtraFiles[0]
	assert.Equal(t, codeFilePath, file.Path)
	assert.Equal(t, "foo bar\n", file.Content)
}

func TestSharedCodeSaveSlice(t *testing.T) {
	t.Parallel()
	targetComponentId := `keboola.python-transformation-v2`
	context, row, rowRecord := createTestFixtures(t, targetComponentId)
	recipe := createLocalSaveRecipe(row, rowRecord)
	codeFilePath := filesystem.Join(context.Naming.SharedCodeFilePath(recipe.Record.Path(), targetComponentId))

	// Set JSON value
	row.Content.Set(model.ShareCodeContentKey, []interface{}{`foo`, `bar`})

	// Create dir
	assert.NoError(t, context.Fs.Mkdir(filesystem.Dir(codeFilePath)))

	// Save to file
	err := NewMapper(context).MapBeforeLocalSave(recipe)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, recipe.ExtraFiles, 1)
	file := recipe.ExtraFiles[0]
	assert.Equal(t, codeFilePath, file.Path)
	assert.Equal(t, "foo\nbar\n", file.Content)
}
