package sharedcode

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeLoadMissingFile(t *testing.T) {
	targetComponentId := `keboola.python-transformation-v2`
	logger, fs, state, _, objectFiles := createTestFixtures(t, targetComponentId)
	err := Load(logger, fs, model.DefaultNaming(), state, objectFiles)
	assert.Error(t, err)
	assert.Equal(t, `missing shared code file "branch/config/row/code.py"`, err.Error())
}

func TestSharedCodeLoadOk(t *testing.T) {
	targetComponentId := `keboola.python-transformation-v2`
	logger, fs, state, row, objectFiles := createTestFixtures(t, targetComponentId)
	naming := model.DefaultNaming()

	// Write file
	codeFilePath := filesystem.Join(naming.SharedCodeFilePath(objectFiles.Record.RelativePath(), targetComponentId))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(codeFilePath, `foo bar`)))

	// Load
	err := Load(logger, fs, naming, state, objectFiles)
	assert.NoError(t, err)
	codeContent, found := row.Content.Get(CodeContentRowJsonKey)
	assert.True(t, found)
	assert.Equal(t, "foo bar\n", codeContent)

	// Path present in related paths
	assert.Equal(t, []string{
		"branch/config/row/code.py",
	}, objectFiles.Record.GetRelatedPaths())
}
