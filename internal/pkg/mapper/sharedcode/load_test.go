package sharedcode_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestSharedCodeMapAfterRemoteLoad(t *testing.T) {
	t.Parallel()
	context := createMapperContext(t)

	variablesConfigId := `123456`
	content := utils.NewOrderedMap()
	content.Set(model.SharedCodeVariablesIdContentKey, variablesConfigId)
	apiObject := &model.ConfigRow{
		ConfigRowKey: model.ConfigRowKey{ComponentId: model.SharedCodeComponentId},
		Content:      content,
	}
	internalObject := apiObject.Clone().(*model.ConfigRow)
	recipe := &model.RemoteLoadRecipe{ApiObject: apiObject, InternalObject: internalObject}

	// Invoke
	assert.Empty(t, apiObject.Relations)
	assert.Empty(t, internalObject.Relations)
	assert.NoError(t, NewMapper(context).MapAfterRemoteLoad(recipe))

	// Api object is not changed
	assert.Empty(t, apiObject.Relations)
	v, found := apiObject.Content.Get(model.SharedCodeVariablesIdContentKey)
	assert.True(t, found)
	assert.Equal(t, variablesConfigId, v)

	// Internal object has new relation + content without variables ID
	assert.Equal(t, model.Relations{
		&model.SharedCodeVariablesFromRelation{
			VariablesId: variablesConfigId,
		},
	}, internalObject.Relations)
	_, found = internalObject.Content.Get(model.SharedCodeVariablesIdContentKey)
	assert.False(t, found)
}

func TestSharedCodeLoadMissingFile(t *testing.T) {
	t.Parallel()
	targetComponentId := `keboola.python-transformation-v2`
	context, row, rowRecord := createTestFixtures(t, targetComponentId)
	recipe := createLocalLoadRecipe(row, rowRecord)

	err := NewMapper(context).MapAfterLocalLoad(recipe)
	assert.Error(t, err)
	assert.Equal(t, `missing shared code file "branch/config/row/code.py"`, err.Error())
}

func TestSharedCodeLoadOk(t *testing.T) {
	t.Parallel()
	targetComponentId := `keboola.python-transformation-v2`
	context, row, rowRecord := createTestFixtures(t, targetComponentId)
	recipe := createLocalLoadRecipe(row, rowRecord)

	// Write file
	codeFilePath := filesystem.Join(context.Naming.SharedCodeFilePath(recipe.Record.Path(), targetComponentId))
	assert.NoError(t, context.Fs.WriteFile(filesystem.CreateFile(codeFilePath, `foo bar`)))

	// Load
	err := NewMapper(context).MapAfterLocalLoad(recipe)
	assert.NoError(t, err)
	codeContent, found := row.Content.Get(model.ShareCodeContentKey)
	assert.True(t, found)
	assert.Equal(t, "foo bar\n", codeContent)

	// Path is present in related paths
	assert.Equal(t, []string{
		"branch/config/row/code.py",
	}, recipe.Record.GetRelatedPaths())
}
