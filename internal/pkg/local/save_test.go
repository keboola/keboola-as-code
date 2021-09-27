package local

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestLocalSaveModel(t *testing.T) {
	manager := newTestLocalManager(t)
	fs := manager.fs

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
	assert.NoError(t, manager.manifest.TrackRecord(record))
	_, found := manager.manifest.GetRecord(record.Key())
	assert.True(t, found)

	// Save
	assert.NoError(t, manager.SaveObject(record, source))

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
	metaFile, err := fs.ReadFile(manager.Naming().MetaFilePath(record.RelativePath()), "")
	assert.NoError(t, err)
	configFile, err := fs.ReadFile(manager.Naming().ConfigFilePath(record.RelativePath()), "")
	assert.NoError(t, err)
	assert.Equal(t, expectedMeta, metaFile.Content)
	assert.Equal(t, expectedConfig, configFile.Content)
}
