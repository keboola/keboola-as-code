package sharedcode_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestSharedCodeMapBeforeRemoteSave(t *testing.T) {
	t.Parallel()
	context := createMapperContext(t)

	variablesConfigId := `123456`
	apiObject := &model.ConfigRow{
		ConfigRowKey: model.ConfigRowKey{ComponentId: model.SharedCodeComponentId},
		Content:      utils.NewOrderedMap(),
	}
	apiObject.AddRelation(&model.SharedCodeVariablesFromRelation{
		VariablesId: variablesConfigId,
	})
	internalObject := apiObject.Clone().(*model.ConfigRow)
	recipe := &model.RemoteSaveRecipe{
		ApiObject:      apiObject,
		InternalObject: internalObject,
		Manifest:       &model.ConfigManifest{},
	}

	// Invoke
	assert.NotEmpty(t, apiObject.Relations)
	assert.NotEmpty(t, internalObject.Relations)
	assert.NoError(t, NewMapper(context).MapBeforeRemoteSave(recipe))

	// Internal object is not changed
	assert.Equal(t, model.Relations{
		&model.SharedCodeVariablesFromRelation{
			VariablesId: variablesConfigId,
		},
	}, internalObject.Relations)
	_, found := internalObject.Content.Get(model.SharedCodeVariablesIdContentKey)
	assert.False(t, found)

	// All relations have been mapped
	assert.Empty(t, apiObject.Relations)

	// Api object contains variables ID in content
	v, found := apiObject.Content.Get(model.SharedCodeVariablesIdContentKey)
	assert.True(t, found)
	assert.Equal(t, variablesConfigId, v)
}

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

func TestSharedCodeSaveOk(t *testing.T) {
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
