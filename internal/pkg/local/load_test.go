package local

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/transformation"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestLocalLoadModel(t *testing.T) {
	t.Parallel()
	manager, _ := newTestLocalManager(t)
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
	target := &fixtures.MockedObject{}
	record := &fixtures.MockedManifest{}
	assert.NoError(t, fs.Mkdir(record.Path()))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(manager.NamingGenerator().MetaFilePath(record.Path()), metaFile)))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(manager.NamingGenerator().ConfigFilePath(record.Path()), configFile)))

	// Load
	found, err := manager.loadObject(record, target)
	assert.True(t, found)
	assert.NoError(t, err)

	// Assert
	config := orderedmap.New()
	config.Set("foo", "bar")
	assert.Equal(t, &fixtures.MockedObject{
		Foo1:   "",
		Foo2:   "",
		Meta1:  "3",
		Meta2:  "4",
		Config: config,
	}, target)
}

func TestLocalLoadModelNotFound(t *testing.T) {
	t.Parallel()
	manager, _ := newTestLocalManager(t)

	// Save files
	target := &fixtures.MockedObject{}
	record := &fixtures.MockedManifest{}

	// Load
	found, err := manager.loadObject(record, target)
	assert.False(t, found)
	assert.Error(t, err)
	assert.Equal(t, "kind \"test\" not found", err.Error())
}

func TestLocalLoadModelInvalidTransformation(t *testing.T) {
	t.Parallel()

	namingTemplate := naming.TemplateWithIds()
	namingRegistry := naming.NewRegistry()
	namingGenerator := naming.NewGenerator(namingTemplate, namingRegistry)

	manager, mapperIst := newTestLocalManager(t)
	fs := manager.fs
	mapperIst.AddMapper(transformation.NewMapper(mapper.Context{
		Logger:          log.NewDebugLogger(),
		State:           manager.state,
		NamingGenerator: namingGenerator,
		Fs:              fs,
	}))

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
	assert.NoError(t, fs.Mkdir(record.Path()))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(namingGenerator.MetaFilePath(record.Path()), metaFile)))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(namingGenerator.DescriptionFilePath(record.Path()), descFile)))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(namingGenerator.ConfigFilePath(record.Path()), configFile)))
	blocksDir := namingGenerator.BlocksDir(record.Path())
	assert.NoError(t, fs.Mkdir(blocksDir))
	block := &model.Block{BlockKey: model.BlockKey{Index: 123}, Name: `block`}
	block.PathInProject = namingGenerator.BlockPath(blocksDir, block)
	assert.NoError(t, fs.Mkdir(block.Path()))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(namingGenerator.MetaFilePath(block.Path()), blockMeta)))
	code := &model.Code{CodeKey: model.CodeKey{Index: 123}, Name: `code`}
	code.PathInProject = namingGenerator.CodePath(block.Path(), code)
	code.CodeFileName = namingGenerator.CodeFileName(component.Id)
	assert.NoError(t, fs.Mkdir(code.Path()))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(namingGenerator.MetaFilePath(code.Path()), codeMeta)))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(namingGenerator.CodeFilePath(code), codeContent)))

	// Load
	found, err := manager.loadObject(record, target)
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
	assert.Len(t, target.Transformation.Blocks, 1)
}
