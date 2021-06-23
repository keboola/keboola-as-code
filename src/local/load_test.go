package local

import (
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalLoadModel(t *testing.T) {
	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, ".keboola")
	assert.NoError(t, os.MkdirAll(metadataDir, 0750))

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
	assert.NoError(t, os.MkdirAll(record.GetPaths().RelativePath(), 0750))
	assert.NoError(t, os.WriteFile(filepath.Join(projectDir, record.MetaFilePath()), []byte(metaFile), 0640))
	assert.NoError(t, os.WriteFile(filepath.Join(projectDir, record.ConfigFilePath()), []byte(configFile), 0640))

	// Load
	assert.NoError(t, LoadModel(projectDir, record, target))

	// Assert
	config := utils.EmptyOrderedMap()
	config.Set("foo", "bar")
	assert.Equal(t, &ModelStruct{
		Foo1:   "",
		Foo2:   "",
		Meta1:  "3",
		Meta2:  "4",
		Config: config,
	}, target)
}
