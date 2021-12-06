package local

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestLocalSaveModel(t *testing.T) {
	t.Parallel()
	manager, _ := newTestLocalManager(t)
	fs := manager.fs

	config := orderedmap.New()
	config.Set("foo", "bar")
	record := &fixtures.MockedManifest{}
	source := &fixtures.MockedObject{
		Foo1:   "1",
		Foo2:   "2",
		Meta1:  "3",
		Meta2:  "4",
		Config: config,
	}
	assert.NoError(t, manager.manifest.PersistRecord(record))
	_, found := manager.manifest.GetRecord(record.Key())
	assert.True(t, found)

	// No related paths
	assert.Empty(t, record.RelatedPaths)

	// Save
	assert.NoError(t, manager.saveObject(record, source, model.ChangedFields{}))

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

	// Related paths are updated
	assert.Equal(t, []string{`test/meta.json`, `test/config.json`}, record.RelatedPaths)
}
