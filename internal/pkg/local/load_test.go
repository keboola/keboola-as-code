package local

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestLocalLoadModel(t *testing.T) {
	manager := newTestLocalManager(t)
	fs := manager.fs

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
	assert.NoError(t, fs.Mkdir(record.RelativePath()))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(manager.Naming().MetaFilePath(record.RelativePath()), metaFile)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(manager.Naming().ConfigFilePath(record.RelativePath()), configFile)))

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
	manager := newTestLocalManager(t)

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
	manager := newTestLocalManager(t)
	fs := manager.fs
	componentProvider := manager.state.Components()
	component := &model.Component{
		ComponentKey: model.ComponentKey{Id: "keboola.foo-bar"},
		Type:         model.TransformationType,
	}
	componentProvider.Set(component)

	// Files content
	metaFile := `{foo`
	descFile := `abc`
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
	assert.NoError(t, fs.Mkdir(record.RelativePath()))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(naming.MetaFilePath(record.RelativePath()), metaFile)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(naming.DescriptionFilePath(record.RelativePath()), descFile)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(naming.ConfigFilePath(record.RelativePath()), configFile)))
	blocksDir := naming.BlocksDir(record.RelativePath())
	assert.NoError(t, fs.Mkdir(blocksDir))
	block := &model.Block{BlockKey: model.BlockKey{Index: 123}, Name: `block`}
	block.PathInProject = naming.BlockPath(blocksDir, block)
	assert.NoError(t, fs.Mkdir(block.RelativePath()))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(naming.MetaFilePath(block.RelativePath()), blockMeta)))
	code := &model.Code{CodeKey: model.CodeKey{Index: 123}, Name: `code`}
	code.PathInProject = naming.CodePath(block.RelativePath(), code)
	code.CodeFileName = naming.CodeFileName(component.Id)
	assert.NoError(t, fs.Mkdir(code.RelativePath()))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(naming.MetaFilePath(code.RelativePath()), codeMeta)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(naming.CodeFilePath(code), codeContent)))

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
