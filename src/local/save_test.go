package local

import (
	"github.com/iancoleman/orderedmap"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
	"testing"
)

type ModelStruct struct {
	Foo1   string
	Foo2   string
	Meta1  string                 `json:"myKey" metaFile:"true"`
	Meta2  string                 `metaFile:"true"`
	Config *orderedmap.OrderedMap `configFile:"true"`
}

type MockedRecord struct{}

func (*MockedRecord) Kind() string {
	return "kind"
}
func (*MockedRecord) KindAbbr() string {
	return "K"
}
func (*MockedRecord) UniqueKey(sort string) string {
	return "key"
}

func (*MockedRecord) GetPaths() *manifest.Paths {
	return &manifest.Paths{
		ParentPath: "",
		Path:       "test",
	}
}

func (*MockedRecord) MetaFilePath() string {
	return "meta-file.json"
}

func (*MockedRecord) ConfigFilePath() string {
	return "config-file.json"
}

func TestLocalSaveModel(t *testing.T) {
	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, ".keboola")
	assert.NoError(t, os.MkdirAll(metadataDir, 0750))

	logger, _ := utils.NewDebugLogger()
	m, err := manifest.NewManifest(1, "connection.keboola.com", projectDir, metadataDir)
	assert.NoError(t, err)

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

	// Save
	assert.NoError(t, SaveModel(logger, m, record, source))

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
	assert.Equal(t, expectedMeta, utils.GetFileContent(filepath.Join(projectDir, record.MetaFilePath())))
	assert.Equal(t, expectedConfig, utils.GetFileContent(filepath.Join(projectDir, record.ConfigFilePath())))
}
