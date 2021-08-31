package local

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"keboola-as-code/src/components"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/utils"
)

func TestLocalSaveModel(t *testing.T) {
	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, ".keboola")
	assert.NoError(t, os.MkdirAll(metadataDir, 0750))

	logger, _ := utils.NewDebugLogger()
	m, err := manifest.NewManifest(1, "connection.keboola.com", projectDir, metadataDir)
	assert.NoError(t, err)
	manager := NewManager(logger, m, components.NewProvider(nil))

	config := utils.NewOrderedMap()
	config.Set("foo", "bar")
	record := &MockedRecord{}
	source := &ModelStruct{
		Foo1:   "1",
		Foo2:   "2",
		Meta1:  "3",
		Meta2:  "4",
		Config: config,
	}
	m.TrackRecord(record)
	_, found := m.GetRecord(record.Key())
	assert.True(t, found)

	// Save
	assert.NoError(t, manager.SaveModel(record, source))

	// Meta and config files are saved
	expectedMeta := `{
  "myKey": "3",
  "Meta2": "4"
}
`
	expectedConfig := `{
  "foo": "bar"
}
`
	assert.Equal(t, expectedMeta, utils.GetFileContent(filepath.Join(projectDir, manager.Naming().MetaFilePath(record.RelativePath()))))
	assert.Equal(t, expectedConfig, utils.GetFileContent(filepath.Join(projectDir, manager.Naming().ConfigFilePath(record.RelativePath()))))
}
