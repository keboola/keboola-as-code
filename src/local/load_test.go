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

func TestLocalLoadModel(t *testing.T) {
	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, ".keboola")
	assert.NoError(t, os.MkdirAll(metadataDir, 0750))
	logger, _ := utils.NewDebugLogger()
	m, err := manifest.NewManifest(12345, "connection.keboola.com", projectDir, metadataDir)
	assert.NoError(t, err)
	manager := NewManager(logger, m, components.NewProvider(nil))

	metaFile := `{
  "myKey": "3",
  "Meta2": "4"
}
`
	configFile := `{
  "foo": "bar"
}
`
	// Save files
	target := &ModelStruct{}
	record := &MockedRecord{}
	assert.NoError(t, os.MkdirAll(filepath.Join(projectDir, record.RelativePath()), 0750))
	assert.NoError(t, os.WriteFile(filepath.Join(projectDir, manager.Naming().MetaFilePath(record.RelativePath())), []byte(metaFile), 0640))
	assert.NoError(t, os.WriteFile(filepath.Join(projectDir, manager.Naming().ConfigFilePath(record.RelativePath())), []byte(configFile), 0640))

	// Load
	found, err := manager.LoadModel(record, target)
	assert.True(t, found)
	assert.NoError(t, err)

	// Assert
	config := utils.NewOrderedMap()
	config.Set("foo", "bar")
	assert.Equal(t, &ModelStruct{
		Foo1:   "",
		Foo2:   "",
		Meta1:  "3",
		Meta2:  "4",
		Config: config,
	}, target)
}

func TestLocalLoadModelNotFound(t *testing.T) {
	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, ".keboola")
	assert.NoError(t, os.MkdirAll(metadataDir, 0750))
	logger, _ := utils.NewDebugLogger()
	m, err := manifest.NewManifest(12345, "connection.keboola.com", projectDir, metadataDir)
	assert.NoError(t, err)
	manager := NewManager(logger, m, components.NewProvider(nil))

	// Save files
	target := &ModelStruct{}
	record := &MockedRecord{}

	// Load
	found, err := manager.LoadModel(record, target)
	assert.False(t, found)
	assert.Error(t, err)
	assert.Equal(t, "kind \"test\" not found", err.Error())
}
