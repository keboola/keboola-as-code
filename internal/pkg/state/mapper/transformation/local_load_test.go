package transformation_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestTransformationLocalMapper_MapAfterLocalLoad_Invalid(t *testing.T) {
	t.Parallel()

	state, d := createLocalStateWithMapper(t)
	fs := d.Fs()
	namingGenerator := state.NamingGenerator()

	// Fixtures
	state.MustAdd(&model.Branch{BranchKey: model.BranchKey{Id: 123}})
	componentId := model.ComponentId("keboola.tr-foo-bar")
	configKey := model.ConfigKey{BranchId: 123, ComponentId: componentId, Id: "456"}
	configPath := model.NewAbsPath("", "config")
	config := &model.Config{ConfigKey: configKey, Content: orderedmap.New()}

	// Files content
	metaFile := `{foo`
	descFile := `abc`
	configFile := ``
	blockMeta := `{"name": "foo1"}`
	codeMeta := `{"name": "foo2"}`
	codeContent := `SELECT 1`

	// Save files
	state.NamingRegistry().MustAttach(configKey, configPath)
	assert.NoError(t, fs.Mkdir(configPath.String()))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(namingGenerator.MetaFilePath(configPath), metaFile)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(namingGenerator.DescriptionFilePath(configPath), descFile)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(namingGenerator.ConfigFilePath(configPath), configFile)))

	blocksDir := namingGenerator.BlocksDir(configPath)
	assert.NoError(t, fs.Mkdir(blocksDir.String()))
	block := &model.Block{BlockKey: model.BlockKey{Index: 123}, Name: `block`}
	blockPath, err := state.GetPath(block)
	assert.NoError(t, err)
	assert.NoError(t, fs.Mkdir(blockPath.String()))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(namingGenerator.MetaFilePath(blockPath), blockMeta)))

	code := &model.Code{CodeKey: model.CodeKey{Index: 123}, Name: `code`}
	codePath, err := state.GetPath(code)
	assert.NoError(t, err)
	code.CodeFileName = namingGenerator.CodeFileName(componentId)
	assert.NoError(t, fs.Mkdir(code.String()))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(namingGenerator.MetaFilePath(codePath), codeMeta)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(namingGenerator.CodeFilePath(code), codeContent)))

	// Load
	recipe := model.NewLocalLoadRecipe(d.FileLoader(), configPath, config)
	err = state.Mapper().MapAfterLocalLoad(recipe)

	// Error is reported
	assert.Error(t, err)
	expectedErr := `
- config metadata file "config/meta.json" is invalid:
  - invalid character 'f' looking for beginning of object key string, offset: 2
- config file "config/config.json" is invalid:
  - empty, please use "{}" for an empty JSON
`
	assert.Equal(t, strings.TrimSpace(expectedErr), err.Error())

	// Invalid config is not set to the state
	_, found := state.Get(configKey)
	assert.False(t, found)
}

func TestTransformationLocalMapper_MapAfterLocalLoad_MissingBlockMetaFile(t *testing.T) {
	t.Parallel()
	state, d := createLocalStateWithMapper(t)
	fs := d.Fs()
	logger := d.DebugLogger()

	// Fixtures
	config, configPath := createTestFixtures(t, "keboola.snowflake-transformation")

	// Create files/dirs
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)
	assert.NoError(t, fs.Mkdir(blocksDir))
	block1 := filesystem.Join(blocksDir, `001-block-1`)
	assert.NoError(t, fs.Mkdir(block1))

	// Load, assert
	recipe := model.NewLocalLoadRecipe(d.FileLoader(), configPath, config)
	err := state.Mapper().MapAfterLocalLoad(recipe)
	assert.Error(t, err)
	assert.Equal(t, `missing block metadata file "branch/config/blocks/001-block-1/meta.json"`, err.Error())
	assert.Empty(t, logger.WarnAndErrorMessages())
}

func TestTransformationLocalMapper_MapAfterLocalLoad_MissingCodeMetaFile(t *testing.T) {
	t.Parallel()
	state, d := createLocalStateWithMapper(t)
	fs := d.Fs()
	logger := d.DebugLogger()

	// Fixtures
	config, configPath := createTestFixtures(t, "keboola.snowflake-transformation")

	// Create files/dirs
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)
	assert.NoError(t, fs.Mkdir(blocksDir))
	block1 := filesystem.Join(blocksDir, `001-block-1`)
	assert.NoError(t, fs.Mkdir(block1))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(block1, `meta.json`), `{"name": "001"}`)))
	block1Code1 := filesystem.Join(block1, `001-code-1`)
	assert.NoError(t, fs.Mkdir(block1Code1))

	// Load, assert
	recipe := model.NewLocalLoadRecipe(d.FileLoader(), configPath, config)
	err := state.Mapper().MapAfterLocalLoad(recipe)
	assert.Error(t, err)
	assert.Equal(t, strings.Join([]string{
		`- missing code metadata file "branch/config/blocks/001-block-1/001-code-1/meta.json"`,
		`- missing code file in "branch/config/blocks/001-block-1/001-code-1"`,
	}, "\n"), err.Error())
	assert.Empty(t, logger.WarnAndErrorMessages())
}

func TestTransformationLocalMapper_MapAfterLocalLoad_Sql(t *testing.T) {
	t.Parallel()
	state, d := createLocalStateWithMapper(t)
	fs := d.Fs()
	logger := d.DebugLogger()

	// Fixtures
	config, configPath := createTestFixtures(t, "keboola.snowflake-transformation")

	// Create files/dirs
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)
	assert.NoError(t, fs.Mkdir(blocksDir))
	block1 := filesystem.Join(blocksDir, `001-block-1`)
	assert.NoError(t, fs.Mkdir(block1))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(block1, `meta.json`), `{"name": "001"}`)))
	block1Code1 := filesystem.Join(block1, `001-code-1`)
	assert.NoError(t, fs.Mkdir(block1Code1))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(block1Code1, `meta.json`), `{"name": "001-001"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(block1Code1, `code.sql`), `SELECT 1`)))
	block1Code2 := filesystem.Join(blocksDir, `001-block-1`, `002-code-2`)
	assert.NoError(t, fs.Mkdir(block1Code2))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(block1Code2, `meta.json`), `{"name": "001-002"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(block1Code2, `code.sql`), `SELECT 1; SELECT 2;`)))
	block2 := filesystem.Join(blocksDir, `002-block-2`)
	assert.NoError(t, fs.Mkdir(block2))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(block2, `meta.json`), `{"name": "002"}`)))
	block2Code1 := filesystem.Join(block2, `002-code-1`)
	assert.NoError(t, fs.Mkdir(block2Code1))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(block2Code1, `meta.json`), `{"name": "002-001"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(block2Code1, `code.sql`), `SELECT 3`)))
	block3 := filesystem.Join(blocksDir, `003-block-3`)
	assert.NoError(t, fs.Mkdir(block3))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(block3, `meta.json`), `{"name": "003"}`)))

	// Load
	recipe := model.NewLocalLoadRecipe(d.FileLoader(), configPath, config)
	assert.NoError(t, state.Mapper().MapAfterLocalLoad(recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Assert
	expected := []*model.Block{
		{
			BlockKey: model.BlockKey{
				BranchId:    123,
				ComponentId: "keboola.snowflake-transformation",
				ConfigId:    `456`,
				Index:       0,
			},
			Name: "001",
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{
						BranchId:    123,
						ComponentId: "keboola.snowflake-transformation",
						ConfigId:    `456`,
						BlockIndex:  0,
						Index:       0,
					},
					Name:         "001-001",
					CodeFileName: `code.sql`,
					Scripts: model.Scripts{
						model.StaticScript{Value: "SELECT 1"},
					},
				},
				{
					CodeKey: model.CodeKey{
						BranchId:    123,
						ComponentId: "keboola.snowflake-transformation",
						ConfigId:    `456`,
						BlockIndex:  0,
						Index:       1,
					},
					Name:         "001-002",
					CodeFileName: `code.sql`,
					Scripts: model.Scripts{
						model.StaticScript{Value: "SELECT 1;"},
						model.StaticScript{Value: "SELECT 2;"},
					},
				},
			},
		},
		{
			BlockKey: model.BlockKey{
				BranchId:    123,
				ComponentId: "keboola.snowflake-transformation",
				ConfigId:    `456`,
				Index:       1,
			},
			Name: "002",
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{
						BranchId:    123,
						ComponentId: "keboola.snowflake-transformation",
						ConfigId:    `456`,
						BlockIndex:  1,
						Index:       0,
					},
					Name:         "002-001",
					CodeFileName: `code.sql`,
					Scripts: model.Scripts{
						model.StaticScript{Value: "SELECT 3"},
					},
				},
			},
		},
		{
			BlockKey: model.BlockKey{
				BranchId:    123,
				ComponentId: "keboola.snowflake-transformation",
				ConfigId:    `456`,
				Index:       2,
			},
			Name:  "003",
			Codes: model.Codes{},
		},
	}
	assert.Equal(t, expected, config.Transformation.Blocks)

	// Check naming registry
	fmt.Printf("%#v", state.NamingRegistry().AllStrings())
	assert.Equal(t, map[string]string{}, state.NamingRegistry().AllStrings())
}

func TestTransformationLocalMapper_MapAfterLocalLoad_Python(t *testing.T) {
	t.Parallel()
	state, d := createLocalStateWithMapper(t)
	fs := d.Fs()
	logger := d.DebugLogger()

	// Fixtures
	config, configPath := createTestFixtures(t, "keboola.python-transformation-v2")

	// Create files/dirs
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)
	assert.NoError(t, fs.Mkdir(blocksDir))
	block1 := filesystem.Join(blocksDir, `001-block-1`)
	assert.NoError(t, fs.Mkdir(block1))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(block1, `meta.json`), `{"name": "001"}`)))
	block1Code1 := filesystem.Join(block1, `001-code-1`)
	assert.NoError(t, fs.Mkdir(block1Code1))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(block1Code1, `meta.json`), `{"name": "001-001"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(block1Code1, `code.py`), `print('1')`)))
	block1Code2 := filesystem.Join(blocksDir, `001-block-1`, `002-code-2`)
	assert.NoError(t, fs.Mkdir(block1Code2))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(block1Code2, `meta.json`), `{"name": "001-002"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(block1Code2, `code.py`), "print('1')\nprint('2')")))
	block2 := filesystem.Join(blocksDir, `002-block-2`)
	assert.NoError(t, fs.Mkdir(block2))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(block2, `meta.json`), `{"name": "002"}`)))
	block2Code1 := filesystem.Join(block2, `002-code-1`)
	assert.NoError(t, fs.Mkdir(block2Code1))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(block2Code1, `meta.json`), `{"name": "002-001"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(block2Code1, `code.py`), `print('3')`)))
	block3 := filesystem.Join(blocksDir, `003-block-3`)
	assert.NoError(t, fs.Mkdir(block3))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(block3, `meta.json`), `{"name": "003"}`)))

	// Load
	recipe := model.NewLocalLoadRecipe(d.FileLoader(), configPath, config)
	assert.NoError(t, state.Mapper().MapAfterLocalLoad(recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Assert
	expected := []*model.Block{
		{
			BlockKey: model.BlockKey{
				BranchId:    123,
				ComponentId: "keboola.python-transformation-v2",
				ConfigId:    `456`,
				Index:       0,
			},
			Name: "001",
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{
						BranchId:    123,
						ComponentId: "keboola.python-transformation-v2",
						ConfigId:    `456`,
						BlockIndex:  0,
						Index:       0,
					},
					Name:         "001-001",
					CodeFileName: `code.py`,
					Scripts: model.Scripts{
						model.StaticScript{Value: "print('1')"},
					},
				},
				{
					CodeKey: model.CodeKey{
						BranchId:    123,
						ComponentId: "keboola.python-transformation-v2",
						ConfigId:    `456`,
						BlockIndex:  0,
						Index:       1,
					},
					Name:         "001-002",
					CodeFileName: `code.py`,
					Scripts: model.Scripts{
						model.StaticScript{Value: "print('1')\nprint('2')"},
					},
				},
			},
		},
		{
			BlockKey: model.BlockKey{
				BranchId:    123,
				ComponentId: "keboola.python-transformation-v2",
				ConfigId:    `456`,
				Index:       1,
			},
			Name: "002",
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{
						BranchId:    123,
						ComponentId: "keboola.python-transformation-v2",
						ConfigId:    `456`,
						BlockIndex:  1,
						Index:       0,
					},
					Name:         "002-001",
					CodeFileName: `code.py`,
					Scripts: model.Scripts{
						model.StaticScript{Value: "print('3')"},
					},
				},
			},
		},
		{
			BlockKey: model.BlockKey{
				BranchId:    123,
				ComponentId: "keboola.python-transformation-v2",
				ConfigId:    `456`,
				Index:       2,
			},

			Name:  "003",
			Codes: model.Codes{},
		},
	}
	assert.Equal(t, expected, config.Transformation.Blocks)

	// Check naming registry
	fmt.Printf("%#v", state.NamingRegistry().AllStrings())
	assert.Equal(t, map[string]string{}, state.NamingRegistry().AllStrings())
}
