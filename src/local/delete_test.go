package local

import (
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalDeleteModel(t *testing.T) {
	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, ".keboola")
	assert.NoError(t, os.MkdirAll(metadataDir, 0750))

	logger, _ := utils.NewDebugLogger()
	m, err := manifest.NewManifest(1, "connection.keboola.com", projectDir, metadataDir)
	assert.NoError(t, err)

	metaFile := `{
  "myKey": "3",
  "Meta2": "4"
}
`
	configFile := `{
  "foo": "bar"
}
`
	target := &ModelStruct{}
	record := &MockedRecord{}
	m.AddRecord(record)
	_, found := m.GetRecord(record.UniqueKey(""))
	assert.True(t, found)

	// Save files
	dirAbs := record.GetPaths().RelativePath()
	metaFileAbs := filepath.Join(projectDir, record.MetaFilePath())
	configFileAbs := filepath.Join(projectDir, record.ConfigFilePath())
	assert.NoError(t, os.MkdirAll(dirAbs, 0750))
	assert.NoError(t, os.WriteFile(metaFileAbs, []byte(metaFile), 0640))
	assert.NoError(t, os.WriteFile(configFileAbs, []byte(configFile), 0640))
	assert.NoError(t, DeleteModel(logger, m, record, target))

	// Assert
	_, found = m.GetRecord(record.UniqueKey(""))
	assert.False(t, found)
	assert.NoFileExists(t, metaFileAbs)
	assert.NoFileExists(t, configFileAbs)
	assert.NoFileExists(t, dirAbs)
}
