package transformation_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
)

func TestTransformationLocalMapper_MapAfterLocalLoad_Invalid(t *testing.T) {
	t.Parallel()

	state, d := createLocalStateWithMapper(t)
	fs := d.Fs()

	// Fixtures
	config, configPath := createTestFixtures(t, "keboola.snowflake-transformation", state)
	state.NamingRegistry().MustAttach(config.Key(), configPath)

	// Files content
	blockMeta := `{foo`
	codeMeta := `{bar`
	codeContent := `SELECT 1`

	// Save block
	blockPath := filesystem.Join(configPath.String(), naming.BlocksDir, "my-block-1")
	blockMetaFilePath := filesystem.Join(blockPath, naming.MetaFile)
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(blockMetaFilePath, blockMeta)))

	// Save code
	codePath := filesystem.Join(blockPath, "my-code-1")
	codeFilePath := filesystem.Join(codePath, naming.CodeFileName+".sql")
	codeMetaFilePath := filesystem.Join(codePath, naming.MetaFile)
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(codeFilePath, codeContent)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(codeMetaFilePath, codeMeta)))

	// Load
	recipe := model.NewLocalLoadRecipe(d.FileLoader(), configPath, config)
	err := state.Mapper().MapAfterLocalLoad(recipe)

	// Error is reported
	assert.Error(t, err)
	expectedErr := `
invalid block "my-block-1":
  - block metadata file "branch/config/blocks/my-block-1/meta.json" is invalid:
    - invalid character 'f' looking for beginning of object key string, offset: 2
  - invalid code "my-code-1":
    - code metadata file "branch/config/blocks/my-block-1/my-code-1/meta.json" is invalid:
      - invalid character 'b' looking for beginning of object key string, offset: 2
`
	assert.Equal(t, strings.TrimSpace(expectedErr), err.Error())
}

func TestTransformationLocalMapper_MapAfterLocalLoad_MissingBlockMetaFile(t *testing.T) {
	t.Parallel()
	state, d := createLocalStateWithMapper(t)
	fs := d.Fs()
	logger := d.DebugLogger()

	// Fixtures
	config, configPath := createTestFixtures(t, "keboola.snowflake-transformation", state)

	// Create empty dirs
	blocksDir := filesystem.Join(configPath.String(), naming.BlocksDir)
	assert.NoError(t, fs.Mkdir(blocksDir))
	assert.NoError(t, fs.Mkdir(filesystem.Join(blocksDir, `001-block-1`)))

	// Load, assert
	recipe := model.NewLocalLoadRecipe(d.FileLoader(), configPath, config)
	err := state.Mapper().MapAfterLocalLoad(recipe)
	assert.Error(t, err)
	expectedErr := `
invalid block "001-block-1":
  - missing block metadata file "branch/config/blocks/001-block-1/meta.json"
`
	assert.Equal(t, strings.TrimSpace(expectedErr), err.Error())
	assert.Empty(t, logger.WarnAndErrorMessages())
}

func TestTransformationLocalMapper_MapAfterLocalLoad_MissingCodeMetaFile(t *testing.T) {
	t.Parallel()
	state, d := createLocalStateWithMapper(t)
	fs := d.Fs()
	logger := d.DebugLogger()

	// Fixtures
	config, configPath := createTestFixtures(t, "keboola.snowflake-transformation", state)

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
	expectedErr := `
invalid block "001-block-1": invalid code "001-code-1":
  - missing code metadata file "branch/config/blocks/001-block-1/001-code-1/meta.json"
  - missing code file in "branch/config/blocks/001-block-1/001-code-1"
`
	assert.Equal(t, strings.TrimSpace(expectedErr), err.Error())
	assert.Empty(t, logger.WarnAndErrorMessages())
}

func TestTransformationLocalMapper_MapAfterLocalLoad_Sql(t *testing.T) {
	t.Parallel()
	state, d := createLocalStateWithMapper(t)
	fs := d.Fs()
	logger := d.DebugLogger()

	// Fixtures
	config, configPath := createTestFixtures(t, "keboola.snowflake-transformation", state)

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
			BlockKey: model.BlockKey{Parent: config.ConfigKey, Index: 0},
			Name:     "001",
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{Parent: model.BlockKey{Parent: config.ConfigKey, Index: 0}, Index: 0},
					Name:    "001-001",
					Scripts: model.Scripts{
						model.StaticScript{Value: "SELECT 1"},
					},
				},
				{
					CodeKey: model.CodeKey{Parent: model.BlockKey{Parent: config.ConfigKey, Index: 0}, Index: 1},
					Name:    "001-002",
					Scripts: model.Scripts{
						model.StaticScript{Value: "SELECT 1;"},
						model.StaticScript{Value: "SELECT 2;"},
					},
				},
			},
		},
		{
			BlockKey: model.BlockKey{Parent: config.ConfigKey, Index: 1},
			Name:     "002",
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{Parent: model.BlockKey{Parent: config.ConfigKey, Index: 1}, Index: 0},
					Name:    "002-001",
					Scripts: model.Scripts{
						model.StaticScript{Value: "SELECT 3"},
					},
				},
			},
		},
		{
			BlockKey: model.BlockKey{Parent: config.ConfigKey, Index: 2},
			Name:     "003",
			Codes:    model.Codes{},
		},
	}
	assert.Equal(t, expected, config.Transformation.Blocks)

	// Check naming registry
	assert.Equal(t, map[string]string{
		`branch "123"`: `branch`,
		`config "branch:123/component:keboola.snowflake-transformation/config:456"`:              `branch/config`,
		`block "branch:123/component:keboola.snowflake-transformation/config:456/block:0"`:       `branch/config/blocks/001-block-1`,
		`code "branch:123/component:keboola.snowflake-transformation/config:456/block:0/code:0"`: `branch/config/blocks/001-block-1/001-code-1`,
		`code "branch:123/component:keboola.snowflake-transformation/config:456/block:0/code:1"`: `branch/config/blocks/001-block-1/002-code-2`,
		`block "branch:123/component:keboola.snowflake-transformation/config:456/block:1"`:       `branch/config/blocks/002-block-2`,
		`code "branch:123/component:keboola.snowflake-transformation/config:456/block:1/code:0"`: `branch/config/blocks/002-block-2/002-code-1`,
		`block "branch:123/component:keboola.snowflake-transformation/config:456/block:2"`:       `branch/config/blocks/003-block-3`,
	}, state.NamingRegistry().AllStrings())
}

func TestTransformationLocalMapper_MapAfterLocalLoad_Python(t *testing.T) {
	t.Parallel()
	state, d := createLocalStateWithMapper(t)
	fs := d.Fs()
	logger := d.DebugLogger()

	// Fixtures
	config, configPath := createTestFixtures(t, "keboola.python-transformation-v2", state)

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
			BlockKey: model.BlockKey{Parent: config.ConfigKey, Index: 0},
			Name:     "001",
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{Parent: model.BlockKey{Parent: config.ConfigKey, Index: 0}, Index: 0},
					Name:    "001-001",
					Scripts: model.Scripts{
						model.StaticScript{Value: "print('1')"},
					},
				},
				{
					CodeKey: model.CodeKey{Parent: model.BlockKey{Parent: config.ConfigKey, Index: 0}, Index: 1},
					Name:    "001-002",
					Scripts: model.Scripts{
						model.StaticScript{Value: "print('1')\nprint('2')"},
					},
				},
			},
		},
		{
			BlockKey: model.BlockKey{Parent: config.ConfigKey, Index: 1},
			Name:     "002",
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{Parent: model.BlockKey{Parent: config.ConfigKey, Index: 1}, Index: 0},
					Name:    "002-001",
					Scripts: model.Scripts{
						model.StaticScript{Value: "print('3')"},
					},
				},
			},
		},
		{
			BlockKey: model.BlockKey{Parent: config.ConfigKey, Index: 2},
			Name:     "003",
			Codes:    model.Codes{},
		},
	}
	assert.Equal(t, expected, config.Transformation.Blocks)

	// Check naming registry
	assert.Equal(t, map[string]string{
		`branch "123"`: `branch`,
		`config "branch:123/component:keboola.python-transformation-v2/config:456"`:              `branch/config`,
		`block "branch:123/component:keboola.python-transformation-v2/config:456/block:0"`:       `branch/config/blocks/001-block-1`,
		`code "branch:123/component:keboola.python-transformation-v2/config:456/block:0/code:0"`: `branch/config/blocks/001-block-1/001-code-1`,
		`code "branch:123/component:keboola.python-transformation-v2/config:456/block:0/code:1"`: `branch/config/blocks/001-block-1/002-code-2`,
		`block "branch:123/component:keboola.python-transformation-v2/config:456/block:1"`:       `branch/config/blocks/002-block-2`,
		`code "branch:123/component:keboola.python-transformation-v2/config:456/block:1/code:0"`: `branch/config/blocks/002-block-2/002-code-1`,
		`block "branch:123/component:keboola.python-transformation-v2/config:456/block:2"`:       `branch/config/blocks/003-block-3`,
	}, state.NamingRegistry().AllStrings())
}
