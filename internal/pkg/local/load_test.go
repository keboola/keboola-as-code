package local

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"keboola-as-code/src/components"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
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

func TestLocalLoadModelInvalidTransformation(t *testing.T) {
	// Mocked component
	componentProvider := components.NewProvider(nil)
	component := &model.Component{
		ComponentKey: model.ComponentKey{Id: "keboola.foo-bar"},
		Type:         model.TransformationType,
	}
	componentProvider.Set(component)

	// Mocked project
	projectDir := t.TempDir()
	metadataDir := filepath.Join(projectDir, ".keboola")
	assert.NoError(t, os.MkdirAll(metadataDir, 0750))
	logger, _ := utils.NewDebugLogger()
	m, err := manifest.NewManifest(12345, "connection.keboola.com", projectDir, metadataDir)
	assert.NoError(t, err)
	manager := NewManager(logger, m, componentProvider)

	// Files content
	metaFile := `{foo`
	configFile := ``
	blockMeta := `{"name": "foo1"}`
	codeMeta := `{"name": "foo2"}`
	codeContent := `SELECT 1`

	// Save files
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: component.Id,
		Id:          "456",
	}
	target := &model.Config{
		ConfigKey: configKey,
	}
	record := &model.ConfigManifest{
		ConfigKey: configKey,
		Paths:     model.Paths{PathInProject: model.PathInProject{ObjectPath: "config"}},
	}
	naming := manager.Naming()
	assert.NoError(t, os.MkdirAll(filepath.Join(projectDir, record.RelativePath()), 0750))
	assert.NoError(t, os.WriteFile(filepath.Join(projectDir, naming.MetaFilePath(record.RelativePath())), []byte(metaFile), 0640))
	assert.NoError(t, os.WriteFile(filepath.Join(projectDir, naming.ConfigFilePath(record.RelativePath())), []byte(configFile), 0640))
	blocksDir := naming.BlocksDir(record.RelativePath())
	assert.NoError(t, os.MkdirAll(filepath.Join(projectDir, blocksDir), 0750))
	block := &model.Block{BlockKey: model.BlockKey{Index: 123}, Name: `block`}
	block.PathInProject = naming.BlockPath(blocksDir, block)
	assert.NoError(t, os.MkdirAll(filepath.Join(projectDir, block.RelativePath()), 0750))
	assert.NoError(t, os.WriteFile(filepath.Join(projectDir, naming.MetaFilePath(block.RelativePath())), []byte(blockMeta), 0640))
	code := &model.Code{CodeKey: model.CodeKey{Index: 123}, Name: `code`}
	code.PathInProject = naming.CodePath(block.RelativePath(), code)
	code.CodeFileName = naming.CodeFileName(component.Id)
	assert.NoError(t, os.MkdirAll(filepath.Join(projectDir, code.RelativePath()), 0750))
	assert.NoError(t, os.WriteFile(filepath.Join(projectDir, naming.MetaFilePath(code.RelativePath())), []byte(codeMeta), 0640))
	assert.NoError(t, os.WriteFile(filepath.Join(projectDir, naming.CodeFilePath(code)), []byte(codeContent), 0640))

	// Load
	found, err := manager.LoadModel(record, target)
	assert.True(t, found)

	// Files are not valid
	assert.Error(t, err)
	expectedErr := `
- config metadata file "config/meta.json" is invalid:
	- invalid character 'f' looking for beginning of object key string, offset: 2
- config file "config/config.json" is invalid:
	- empty, please use "{}" for an empty JSON
`
	assert.Equal(t, strings.TrimSpace(expectedErr), err.Error())

	// But the blocks are parsed, no crash
	assert.Len(t, target.Blocks, 1)
}
