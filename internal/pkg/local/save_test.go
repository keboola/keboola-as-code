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
	assert.NoError(t, m.TrackRecord(record))
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
