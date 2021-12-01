package codes_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/codes"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeLegacyRemoteCodeContent(t *testing.T) {
	t.Parallel()
	context, row, _ := createTestFixtures(t, `keboola.snowflake-transformation`)
	row.Content.Set(model.SharedCodeContentKey, "SELECT 1; \n  SELECT 2; \n ")

	event := model.OnObjectsLoadEvent{
		StateType:  model.StateTypeRemote,
		NewObjects: []model.Object{row},
		AllObjects: context.State.RemoteObjects(),
	}
	assert.NoError(t, NewMapper(context).OnObjectsLoad(event))

	v, found := row.Content.Get(model.SharedCodeContentKey)
	assert.True(t, found)
	assert.Equal(t, []interface{}{"SELECT 1;", "SELECT 2;"}, v)
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
	codeFilePath := filesystem.Join(context.Naming.SharedCodeFilePath(recipe.ObjectManifest.Path(), targetComponentId))
	assert.NoError(t, context.Fs.WriteFile(filesystem.CreateFile(codeFilePath, `foo bar`)))

	// Load
	err := NewMapper(context).MapAfterLocalLoad(recipe)
	assert.NoError(t, err)
	codeContent, found := row.Content.Get(model.SharedCodeContentKey)
	assert.True(t, found)
	assert.Equal(t, []interface{}{"foo bar"}, codeContent)

	// Path is present in related paths
	assert.Equal(t, []string{
		"branch/config/row/code.py",
	}, recipe.ObjectManifest.GetRelatedPaths())
}
