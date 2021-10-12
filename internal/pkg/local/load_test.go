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
	t.Parallel()
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
	assert.NoError(t, fs.Mkdir(record.Path()))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(manager.Naming().MetaFilePath(record.Path()), metaFile)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(manager.Naming().ConfigFilePath(record.Path()), configFile)))

	// Load
	found, err := manager.LoadObject(record, target)
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
	t.Parallel()
	manager := newTestLocalManager(t)

	// Save files
	target := &ModelStruct{}
	record := &MockedRecord{}

	// Load
	found, err := manager.LoadObject(record, target)
	assert.False(t, found)
	assert.Error(t, err)
	assert.Equal(t, "kind \"test\" not found", err.Error())
}

func TestLocalLoadModelInvalidTransformation(t *testing.T) {
	t.Parallel()
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
	assert.NoError(t, fs.Mkdir(record.Path()))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(naming.MetaFilePath(record.Path()), metaFile)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(naming.DescriptionFilePath(record.Path()), descFile)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(naming.ConfigFilePath(record.Path()), configFile)))
	blocksDir := naming.BlocksDir(record.Path())
	assert.NoError(t, fs.Mkdir(blocksDir))
	block := &model.Block{BlockKey: model.BlockKey{Index: 123}, Name: `block`}
	block.PathInProject = naming.BlockPath(blocksDir, block)
	assert.NoError(t, fs.Mkdir(block.Path()))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(naming.MetaFilePath(block.Path()), blockMeta)))
	code := &model.Code{CodeKey: model.CodeKey{Index: 123}, Name: `code`}
	code.PathInProject = naming.CodePath(block.Path(), code)
	code.CodeFileName = naming.CodeFileName(component.Id)
	assert.NoError(t, fs.Mkdir(code.Path()))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(naming.MetaFilePath(code.Path()), codeMeta)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(naming.CodeFilePath(code), codeContent)))

	// Load
	found, err := manager.LoadObject(record, target)
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
