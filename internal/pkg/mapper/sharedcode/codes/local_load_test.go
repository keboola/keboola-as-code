package codes_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/codes"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeLoadMissingFile(t *testing.T) {
	t.Parallel()
	targetComponentId := `keboola.python-transformation-v2`
	context, rowState := createTestFixtures(t, targetComponentId)
	recipe := createLocalLoadRecipe(rowState)

	err := NewMapper(context).MapAfterLocalLoad(recipe)
	assert.Error(t, err)
	assert.Equal(t, `missing shared code file "branch/config/row/code.py"`, err.Error())
}

func TestSharedCodeLoadOk(t *testing.T) {
	t.Parallel()
	targetComponentId := `keboola.python-transformation-v2`
	context, rowState := createTestFixtures(t, targetComponentId)
	recipe := createLocalLoadRecipe(rowState)

	// Write file
	codeFilePath := filesystem.Join(context.Naming.SharedCodeFilePath(recipe.ObjectManifest.Path(), targetComponentId))
	assert.NoError(t, context.Fs.WriteFile(filesystem.CreateFile(codeFilePath, `foo bar`)))

	// Load
	err := NewMapper(context).MapAfterLocalLoad(recipe)
	assert.NoError(t, err)
	codeContent, found := rowState.Local.Content.Get(model.SharedCodeContentKey)
	assert.True(t, found)
	assert.Equal(t, []interface{}{"foo bar"}, codeContent)

	// Path is present in related paths
	assert.Equal(t, []string{
		"branch/config/row/code.py",
	}, recipe.ObjectManifest.GetRelatedPaths())
}
