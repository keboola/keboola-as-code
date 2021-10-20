package sharedcode_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeLoadMissingFile(t *testing.T) {
	t.Parallel()
	targetComponentId := `keboola.python-transformation-v2`
	context, row, rowRecord := createTestFixtures(t, targetComponentId)
	recipe := createLocalLoadRecipe(row, rowRecord)

	err := NewMapper(context).AfterLocalLoad(recipe)
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
	err := NewMapper(context).AfterLocalLoad(recipe)
	assert.NoError(t, err)
	codeContent, found := row.Content.Get(model.ShareCodeContentKey)
	assert.True(t, found)
	assert.Equal(t, "foo bar\n", codeContent)

	// Path is present in related paths
	assert.Equal(t, []string{
		"branch/config/row/code.py",
	}, recipe.Record.GetRelatedPaths())
}
