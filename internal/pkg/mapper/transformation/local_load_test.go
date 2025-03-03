package transformation_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/corefiles"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/transformation"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

func TestLoadTransformationInvalidConfigAndMeta(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	d := dependencies.NewMocked(t, t.Context())
	state := d.MockedState()
	state.Mapper().AddMapper(corefiles.NewMapper(state))
	state.Mapper().AddMapper(transformation.NewMapper(state))

	fs := state.ObjectsRoot()
	namingGenerator := state.NamingGenerator()

	// Files content
	metaFile := `{foo`
	descFile := `abc`
	configFile := ``
	blockMeta := `{"name": "foo1"}`
	codeMeta := `{"name": "foo2"}`
	codeContent := `SELECT 1`

	// Save files
	configKey := model.ConfigKey{
		BranchID:    123,
		ComponentID: "foo.bar",
		ID:          "456",
	}
	record := &model.ConfigManifest{
		ConfigKey: configKey,
		Paths:     model.Paths{AbsPath: model.AbsPath{RelativePath: "config"}},
	}
	require.NoError(t, fs.Mkdir(ctx, record.Path()))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(namingGenerator.MetaFilePath(record.Path()), metaFile)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(namingGenerator.DescriptionFilePath(record.Path()), descFile)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(namingGenerator.ConfigFilePath(record.Path()), configFile)))
	blocksDir := namingGenerator.BlocksDir(record.Path())
	require.NoError(t, fs.Mkdir(ctx, blocksDir))
	block := &model.Block{BlockKey: model.BlockKey{Index: 123}, Name: `block`}
	block.AbsPath = namingGenerator.BlockPath(blocksDir, block)
	require.NoError(t, fs.Mkdir(ctx, block.Path()))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(namingGenerator.MetaFilePath(block.Path()), blockMeta)))
	code := &model.Code{CodeKey: model.CodeKey{Index: 123}, Name: `code`}
	code.AbsPath = namingGenerator.CodePath(block.Path(), code)
	code.CodeFileName = namingGenerator.CodeFileName("foo.bar")
	require.NoError(t, fs.Mkdir(ctx, code.Path()))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(namingGenerator.MetaFilePath(code.Path()), codeMeta)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(namingGenerator.CodeFilePath(code), codeContent)))

	// Set parent
	require.NoError(t, state.Set(&model.BranchState{
		BranchManifest: &model.BranchManifest{BranchKey: configKey.BranchKey()},
		Local:          &model.Branch{BranchKey: configKey.BranchKey()},
	}))

	// Load
	uow := state.LocalManager().NewUnitOfWork(ctx)
	uow.LoadObject(record, model.NoFilter())
	err := uow.Invoke()

	// Error is reported
	require.Error(t, err)
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

func TestLoadTransformationMissingBlockMetaSql(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	fs := state.ObjectsRoot()
	logger := d.DebugLogger()
	ctx := t.Context()

	configState := createTestFixtures(t, "keboola.snowflake-transformation")
	recipe := model.NewLocalLoadRecipe(state.FileLoader(), configState.Manifest(), configState.Local)

	// Create files/dirs
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)
	require.NoError(t, fs.Mkdir(ctx, blocksDir))
	block1 := filesystem.Join(blocksDir, `001-block-1`)
	require.NoError(t, fs.Mkdir(ctx, block1))

	// Load, assert
	err := state.Mapper().MapAfterLocalLoad(t.Context(), recipe)
	require.Error(t, err)
	assert.Equal(t, `missing block metadata file "branch/config/blocks/001-block-1/meta.json"`, err.Error())
	assert.Empty(t, logger.WarnAndErrorMessages())
}

func TestLoadTransformationMissingCodeMeta(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	fs := state.ObjectsRoot()
	logger := d.DebugLogger()
	ctx := t.Context()

	configState := createTestFixtures(t, "keboola.snowflake-transformation")
	recipe := model.NewLocalLoadRecipe(state.FileLoader(), configState.Manifest(), configState.Local)

	// Create files/dirs
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)
	require.NoError(t, fs.Mkdir(ctx, blocksDir))
	block1 := filesystem.Join(blocksDir, `001-block-1`)
	require.NoError(t, fs.Mkdir(ctx, block1))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(block1, `meta.json`), `{"name": "001"}`)))
	block1Code1 := filesystem.Join(block1, `001-code-1`)
	require.NoError(t, fs.Mkdir(ctx, block1Code1))

	// Load, assert
	err := state.Mapper().MapAfterLocalLoad(t.Context(), recipe)
	require.Error(t, err)
	assert.Equal(t, strings.Join([]string{
		`- missing code metadata file "branch/config/blocks/001-block-1/001-code-1/meta.json"`,
		`- missing code file in "branch/config/blocks/001-block-1/001-code-1"`,
	}, "\n"), err.Error())
	assert.Empty(t, logger.WarnAndErrorMessages())
}

func TestLoadLocalTransformationSql(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	fs := state.ObjectsRoot()
	logger := d.DebugLogger()
	ctx := t.Context()

	configState := createTestFixtures(t, "keboola.snowflake-transformation")
	recipe := model.NewLocalLoadRecipe(state.FileLoader(), configState.Manifest(), configState.Local)

	// Create files/dirs
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)
	require.NoError(t, fs.Mkdir(ctx, blocksDir))
	block1 := filesystem.Join(blocksDir, `001-block-1`)
	require.NoError(t, fs.Mkdir(ctx, block1))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(block1, `meta.json`), `{"name": "001"}`)))
	block1Code1 := filesystem.Join(block1, `001-code-1`)
	require.NoError(t, fs.Mkdir(ctx, block1Code1))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(block1Code1, `meta.json`), `{"name": "001-001"}`)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(block1Code1, `code.sql`), `SELECT 1`)))
	block1Code2 := filesystem.Join(blocksDir, `001-block-1`, `002-code-2`)
	require.NoError(t, fs.Mkdir(ctx, block1Code2))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(block1Code2, `meta.json`), `{"name": "001-002"}`)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(block1Code2, `code.sql`), `SELECT 1; SELECT 2;`)))
	block2 := filesystem.Join(blocksDir, `002-block-2`)
	require.NoError(t, fs.Mkdir(ctx, block2))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(block2, `meta.json`), `{"name": "002"}`)))
	block2Code1 := filesystem.Join(block2, `002-code-1`)
	require.NoError(t, fs.Mkdir(ctx, block2Code1))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(block2Code1, `meta.json`), `{"name": "002-001"}`)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(block2Code1, `code.sql`), `SELECT 3`)))
	block3 := filesystem.Join(blocksDir, `003-block-3`)
	require.NoError(t, fs.Mkdir(ctx, block3))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(block3, `meta.json`), `{"name": "003"}`)))

	// Load
	require.NoError(t, state.Mapper().MapAfterLocalLoad(t.Context(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Assert
	expected := []*model.Block{
		{
			BlockKey: model.BlockKey{
				BranchID:    123,
				ComponentID: "keboola.snowflake-transformation",
				ConfigID:    `456`,
				Index:       0,
			},
			AbsPath: model.NewAbsPath(
				`branch/config/blocks`,
				`001-block-1`,
			),
			Name: "001",
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{
						BranchID:    123,
						ComponentID: "keboola.snowflake-transformation",
						ConfigID:    `456`,
						BlockIndex:  0,
						Index:       0,
					},
					AbsPath: model.NewAbsPath(
						`branch/config/blocks/001-block-1`,
						`001-code-1`,
					),
					Name:         "001-001",
					CodeFileName: `code.sql`,
					Scripts: model.Scripts{
						model.StaticScript{Value: "SELECT 1"},
					},
				},
				{
					CodeKey: model.CodeKey{
						BranchID:    123,
						ComponentID: "keboola.snowflake-transformation",
						ConfigID:    `456`,
						BlockIndex:  0,
						Index:       1,
					},
					AbsPath: model.NewAbsPath(
						`branch/config/blocks/001-block-1`,
						`002-code-2`,
					),
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
				BranchID:    123,
				ComponentID: "keboola.snowflake-transformation",
				ConfigID:    `456`,
				Index:       1,
			},
			AbsPath: model.NewAbsPath(
				`branch/config/blocks`,
				`002-block-2`,
			),
			Name: "002",
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{
						BranchID:    123,
						ComponentID: "keboola.snowflake-transformation",
						ConfigID:    `456`,
						BlockIndex:  1,
						Index:       0,
					},
					AbsPath: model.NewAbsPath(
						`branch/config/blocks/002-block-2`,
						`002-code-1`,
					),
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
				BranchID:    123,
				ComponentID: "keboola.snowflake-transformation",
				ConfigID:    `456`,
				Index:       2,
			},
			AbsPath: model.NewAbsPath(
				`branch/config/blocks`,
				`003-block-3`,
			),
			Name:  "003",
			Codes: model.Codes{},
		},
	}
	assert.Equal(t, expected, configState.Local.Transformation.Blocks)
}

func TestLoadLocalTransformationPy(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	fs := state.ObjectsRoot()
	logger := d.DebugLogger()
	ctx := t.Context()

	configState := createTestFixtures(t, `keboola.python-transformation-v2`)
	recipe := model.NewLocalLoadRecipe(state.FileLoader(), configState.Manifest(), configState.Local)

	// Create files/dirs
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)
	require.NoError(t, fs.Mkdir(ctx, blocksDir))
	block1 := filesystem.Join(blocksDir, `001-block-1`)
	require.NoError(t, fs.Mkdir(ctx, block1))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(block1, `meta.json`), `{"name": "001"}`)))
	block1Code1 := filesystem.Join(block1, `001-code-1`)
	require.NoError(t, fs.Mkdir(ctx, block1Code1))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(block1Code1, `meta.json`), `{"name": "001-001"}`)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(block1Code1, `code.py`), `print('1')`)))
	block1Code2 := filesystem.Join(blocksDir, `001-block-1`, `002-code-2`)
	require.NoError(t, fs.Mkdir(ctx, block1Code2))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(block1Code2, `meta.json`), `{"name": "001-002"}`)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(block1Code2, `code.py`), "print('1')\nprint('2')")))
	block2 := filesystem.Join(blocksDir, `002-block-2`)
	require.NoError(t, fs.Mkdir(ctx, block2))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(block2, `meta.json`), `{"name": "002"}`)))
	block2Code1 := filesystem.Join(block2, `002-code-1`)
	require.NoError(t, fs.Mkdir(ctx, block2Code1))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(block2Code1, `meta.json`), `{"name": "002-001"}`)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(block2Code1, `code.py`), `print('3')`)))
	block3 := filesystem.Join(blocksDir, `003-block-3`)
	require.NoError(t, fs.Mkdir(ctx, block3))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(block3, `meta.json`), `{"name": "003"}`)))

	// Load
	require.NoError(t, state.Mapper().MapAfterLocalLoad(t.Context(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Assert
	expected := []*model.Block{
		{
			BlockKey: model.BlockKey{
				BranchID:    123,
				ComponentID: "keboola.python-transformation-v2",
				ConfigID:    `456`,
				Index:       0,
			},
			AbsPath: model.NewAbsPath(
				`branch/config/blocks`,
				`001-block-1`,
			),
			Name: "001",
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{
						BranchID:    123,
						ComponentID: "keboola.python-transformation-v2",
						ConfigID:    `456`,
						BlockIndex:  0,
						Index:       0,
					},
					AbsPath: model.NewAbsPath(
						`branch/config/blocks/001-block-1`,
						`001-code-1`,
					),
					Name:         "001-001",
					CodeFileName: `code.py`,
					Scripts: model.Scripts{
						model.StaticScript{Value: "print('1')"},
					},
				},
				{
					CodeKey: model.CodeKey{
						BranchID:    123,
						ComponentID: "keboola.python-transformation-v2",
						ConfigID:    `456`,
						BlockIndex:  0,
						Index:       1,
					},
					AbsPath: model.NewAbsPath(
						`branch/config/blocks/001-block-1`,
						`002-code-2`,
					),
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
				BranchID:    123,
				ComponentID: "keboola.python-transformation-v2",
				ConfigID:    `456`,
				Index:       1,
			},
			AbsPath: model.NewAbsPath(
				`branch/config/blocks`,
				`002-block-2`,
			),
			Name: "002",
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{
						BranchID:    123,
						ComponentID: "keboola.python-transformation-v2",
						ConfigID:    `456`,
						BlockIndex:  1,
						Index:       0,
					},
					AbsPath: model.NewAbsPath(
						`branch/config/blocks/002-block-2`,
						`002-code-1`,
					),
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
				BranchID:    123,
				ComponentID: "keboola.python-transformation-v2",
				ConfigID:    `456`,
				Index:       2,
			},
			AbsPath: model.NewAbsPath(
				`branch/config/blocks`,
				`003-block-3`,
			),
			Name:  "003",
			Codes: model.Codes{},
		},
	}
	assert.Equal(t, expected, configState.Local.Transformation.Blocks)
}
