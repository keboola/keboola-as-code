package transformation_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/transformation"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestLoadTransformationMissingBlockMetaSql(t *testing.T) {
	t.Parallel()
	context, configState := createTestFixtures(t, "keboola.snowflake-transformation")
	recipe := fixtures.NewLocalLoadRecipe(configState.Manifest(), configState.Local)
	fs := context.Fs
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)

	// Create files/dirs
	assert.NoError(t, fs.Mkdir(blocksDir))
	block1 := filesystem.Join(blocksDir, `001-block-1`)
	assert.NoError(t, fs.Mkdir(block1))

	// Load, assert
	err := NewMapper(context).MapAfterLocalLoad(recipe)
	assert.Error(t, err)
	assert.Equal(t, `missing block metadata file "branch/config/blocks/001-block-1/meta.json"`, err.Error())
}

func TestLoadTransformationMissingCodeMeta(t *testing.T) {
	t.Parallel()
	context, configState := createTestFixtures(t, "keboola.snowflake-transformation")
	recipe := fixtures.NewLocalLoadRecipe(configState.Manifest(), configState.Local)
	fs := context.Fs
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)

	// Create files/dirs
	assert.NoError(t, fs.Mkdir(blocksDir))
	block1 := filesystem.Join(blocksDir, `001-block-1`)
	assert.NoError(t, fs.Mkdir(block1))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(block1, `meta.json`), `{"name": "001"}`)))
	block1Code1 := filesystem.Join(block1, `001-code-1`)
	assert.NoError(t, fs.Mkdir(block1Code1))

	// Load, assert
	err := NewMapper(context).MapAfterLocalLoad(recipe)
	assert.Error(t, err)
	assert.Equal(t, strings.Join([]string{
		`- missing code metadata file "branch/config/blocks/001-block-1/001-code-1/meta.json"`,
		`- missing code file in "branch/config/blocks/001-block-1/001-code-1"`,
	}, "\n"), err.Error())
}

func TestLoadLocalTransformationSql(t *testing.T) {
	t.Parallel()
	context, configState := createTestFixtures(t, `keboola.snowflake-transformation`)
	recipe := fixtures.NewLocalLoadRecipe(configState.Manifest(), configState.Local)
	fs := context.Fs
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)

	// Create files/dirs
	assert.NoError(t, fs.Mkdir(blocksDir))
	block1 := filesystem.Join(blocksDir, `001-block-1`)
	assert.NoError(t, fs.Mkdir(block1))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(block1, `meta.json`), `{"name": "001"}`)))
	block1Code1 := filesystem.Join(block1, `001-code-1`)
	assert.NoError(t, fs.Mkdir(block1Code1))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(block1Code1, `meta.json`), `{"name": "001-001"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(block1Code1, `code.sql`), `SELECT 1`)))
	block1Code2 := filesystem.Join(blocksDir, `001-block-1`, `002-code-2`)
	assert.NoError(t, fs.Mkdir(block1Code2))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(block1Code2, `meta.json`), `{"name": "001-002"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(block1Code2, `code.sql`), `SELECT 1; SELECT 2;`)))
	block2 := filesystem.Join(blocksDir, `002-block-2`)
	assert.NoError(t, fs.Mkdir(block2))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(block2, `meta.json`), `{"name": "002"}`)))
	block2Code1 := filesystem.Join(block2, `002-code-1`)
	assert.NoError(t, fs.Mkdir(block2Code1))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(block2Code1, `meta.json`), `{"name": "002-001"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(block2Code1, `code.sql`), `SELECT 3`)))
	block3 := filesystem.Join(blocksDir, `003-block-3`)
	assert.NoError(t, fs.Mkdir(block3))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(block3, `meta.json`), `{"name": "003"}`)))

	// Load
	assert.NoError(t, NewMapper(context).MapAfterLocalLoad(recipe))

	// Assert
	expected := []*model.Block{
		{
			BlockKey: model.BlockKey{
				BranchId:    123,
				ComponentId: "keboola.snowflake-transformation",
				ConfigId:    `456`,
				Index:       0,
			},
			PathInProject: model.NewPathInProject(
				`branch/config/blocks`,
				`001-block-1`,
			),
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
					PathInProject: model.NewPathInProject(
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
						BranchId:    123,
						ComponentId: "keboola.snowflake-transformation",
						ConfigId:    `456`,
						BlockIndex:  0,
						Index:       1,
					},
					PathInProject: model.NewPathInProject(
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
				BranchId:    123,
				ComponentId: "keboola.snowflake-transformation",
				ConfigId:    `456`,
				Index:       1,
			},
			PathInProject: model.NewPathInProject(
				`branch/config/blocks`,
				`002-block-2`,
			),
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
					PathInProject: model.NewPathInProject(
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
				BranchId:    123,
				ComponentId: "keboola.snowflake-transformation",
				ConfigId:    `456`,
				Index:       2,
			},
			PathInProject: model.NewPathInProject(
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
	context, configState := createTestFixtures(t, `keboola.python-transformation-v2`)
	recipe := fixtures.NewLocalLoadRecipe(configState.Manifest(), configState.Local)
	fs := context.Fs
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)

	// Create files/dirs
	assert.NoError(t, fs.Mkdir(blocksDir))
	block1 := filesystem.Join(blocksDir, `001-block-1`)
	assert.NoError(t, fs.Mkdir(block1))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(block1, `meta.json`), `{"name": "001"}`)))
	block1Code1 := filesystem.Join(block1, `001-code-1`)
	assert.NoError(t, fs.Mkdir(block1Code1))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(block1Code1, `meta.json`), `{"name": "001-001"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(block1Code1, `code.py`), `print('1')`)))
	block1Code2 := filesystem.Join(blocksDir, `001-block-1`, `002-code-2`)
	assert.NoError(t, fs.Mkdir(block1Code2))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(block1Code2, `meta.json`), `{"name": "001-002"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(block1Code2, `code.py`), "print('1')\nprint('2')")))
	block2 := filesystem.Join(blocksDir, `002-block-2`)
	assert.NoError(t, fs.Mkdir(block2))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(block2, `meta.json`), `{"name": "002"}`)))
	block2Code1 := filesystem.Join(block2, `002-code-1`)
	assert.NoError(t, fs.Mkdir(block2Code1))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(block2Code1, `meta.json`), `{"name": "002-001"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(block2Code1, `code.py`), `print('3')`)))
	block3 := filesystem.Join(blocksDir, `003-block-3`)
	assert.NoError(t, fs.Mkdir(block3))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(block3, `meta.json`), `{"name": "003"}`)))

	// Load
	assert.NoError(t, NewMapper(context).MapAfterLocalLoad(recipe))

	// Assert
	expected := []*model.Block{
		{
			BlockKey: model.BlockKey{
				BranchId:    123,
				ComponentId: "keboola.python-transformation-v2",
				ConfigId:    `456`,
				Index:       0,
			},
			PathInProject: model.NewPathInProject(
				`branch/config/blocks`,
				`001-block-1`,
			),
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
					PathInProject: model.NewPathInProject(
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
						BranchId:    123,
						ComponentId: "keboola.python-transformation-v2",
						ConfigId:    `456`,
						BlockIndex:  0,
						Index:       1,
					},
					PathInProject: model.NewPathInProject(
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
				BranchId:    123,
				ComponentId: "keboola.python-transformation-v2",
				ConfigId:    `456`,
				Index:       1,
			},
			PathInProject: model.NewPathInProject(
				`branch/config/blocks`,
				`002-block-2`,
			),
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
					PathInProject: model.NewPathInProject(
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
				BranchId:    123,
				ComponentId: "keboola.python-transformation-v2",
				ConfigId:    `456`,
				Index:       2,
			},
			PathInProject: model.NewPathInProject(
				`branch/config/blocks`,
				`003-block-3`,
			),
			Name:  "003",
			Codes: model.Codes{},
		},
	}
	assert.Equal(t, expected, configState.Local.Transformation.Blocks)
}
