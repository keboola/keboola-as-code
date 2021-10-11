package sharedcode

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeSaveMissingKey(t *testing.T) {
	t.Parallel()
	targetComponentId := `keboola.python-transformation-v2`
	logger, fs, state, _, objectFiles := createTestFixtures(t, targetComponentId)
	naming := model.DefaultNaming()

	err := Save(logger, fs, naming, state, objectFiles)
	assert.Error(t, err)
	assert.Equal(t, `key "code_content" not found in config row "branch:789/component:keboola.shared-code/config:123/row:456"`, err.Error())
	assert.Len(t, objectFiles.Extra, 0)
}

func TestSharedCodeSaveOk(t *testing.T) {
	t.Parallel()
	targetComponentId := `keboola.python-transformation-v2`
	logger, fs, state, row, objectFiles := createTestFixtures(t, targetComponentId)
	naming := model.DefaultNaming()
	codeFilePath := filesystem.Join(naming.SharedCodeFilePath(objectFiles.Record.Path(), targetComponentId))

	// Set JSON value
	row.Content.Set(model.ShareCodeContentKey, `foo bar`)

	// Create dir
	assert.NoError(t, fs.Mkdir(filesystem.Dir(codeFilePath)))

	// Save to file
	err := Save(logger, fs, naming, state, objectFiles)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, objectFiles.Extra, 1)
	file := objectFiles.Extra[0]
	assert.Equal(t, codeFilePath, file.Path)
	assert.Equal(t, "foo bar\n", file.Content)
}
