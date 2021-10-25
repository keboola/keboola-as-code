package local

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestLocalSaveModel(t *testing.T) {
	t.Parallel()
	manager := newTestLocalManager(t)
	fs := manager.fs

	config := utils.NewOrderedMap()
	config.Set("foo", "bar")
	record := &MockedRecord{}
	source := &MockedObject{
		Foo1:   "1",
		Foo2:   "2",
		Meta1:  "3",
		Meta2:  "4",
		Config: config,
	}
	assert.NoError(t, manager.manifest.PersistRecord(record))
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
	metaFile, err := fs.ReadFile(manager.Naming().MetaFilePath(record.Path()), "")
	assert.NoError(t, err)
	configFile, err := fs.ReadFile(manager.Naming().ConfigFilePath(record.Path()), "")
	assert.NoError(t, err)
	assert.Equal(t, expectedMeta, metaFile.Content)
	assert.Equal(t, expectedConfig, configFile.Content)
}
