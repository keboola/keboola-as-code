package transformation

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/iancoleman/orderedmap"
	"github.com/stretchr/testify/assert"

	"keboola-as-code/src/json"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
)

func TestLoadTransformationMissingBlockMetaSql(t *testing.T) {
	projectDir := t.TempDir()
	blocksDir := filepath.Join(projectDir, `branch`, `config`, `blocks`)

	// Create files/dirs
	assert.NoError(t, os.MkdirAll(blocksDir, 0755))
	block1 := filepath.Join(blocksDir, `001-block-1`)
	assert.NoError(t, os.MkdirAll(block1, 0755))

	// Load, assert
	logger, _ := utils.NewDebugLogger()
	record, target := createTransTestStructs("keboola.snowflake-transformation")
	err := LoadBlocks(projectDir, logger, model.DefaultNaming(), record, target)
	assert.Error(t, err)
	assert.Equal(t, `missing block metadata file "branch/config/blocks/001-block-1/meta.json"`, err.Error())
}

func TestLoadTransformationMissingCodeMeta(t *testing.T) {
	projectDir := t.TempDir()
	blocksDir := filepath.Join(projectDir, `branch`, `config`, `blocks`)

	// Create files/dirs
	assert.NoError(t, os.MkdirAll(blocksDir, 0755))
	block1 := filepath.Join(blocksDir, `001-block-1`)
	assert.NoError(t, os.MkdirAll(block1, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(block1, `meta.json`), []byte(`{"name": "001"}`), 0644))
	block1Code1 := filepath.Join(block1, `001-code-1`)
	assert.NoError(t, os.MkdirAll(block1Code1, 0755))

	// Load, assert
	logger, _ := utils.NewDebugLogger()
	record, target := createTransTestStructs("keboola.snowflake-transformation")
	err := LoadBlocks(projectDir, logger, model.DefaultNaming(), record, target)
	assert.Error(t, err)
	assert.Equal(t, strings.Join([]string{
		`- missing code metadata file "branch/config/blocks/001-block-1/001-code-1/meta.json"`,
		`- missing code file in "branch/config/blocks/001-block-1/001-code-1"`,
	}, "\n"), err.Error())
}

func TestLoadTransformationSql(t *testing.T) {
	projectDir := t.TempDir()
	blocksDir := filepath.Join(projectDir, `branch`, `config`, `blocks`)

	// Create files/dirs
	assert.NoError(t, os.MkdirAll(blocksDir, 0755))
	block1 := filepath.Join(blocksDir, `001-block-1`)
	assert.NoError(t, os.MkdirAll(block1, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(block1, `meta.json`), []byte(`{"name": "001"}`), 0644))
	block1Code1 := filepath.Join(block1, `001-code-1`)
	assert.NoError(t, os.MkdirAll(block1Code1, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(block1Code1, `meta.json`), []byte(`{"name": "001-001"}`), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(block1Code1, `code.sql`), []byte(`SELECT 1`), 0644))
	block1Code2 := filepath.Join(blocksDir, `001-block-1`, `002-code-2`)
	assert.NoError(t, os.MkdirAll(block1Code2, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(block1Code2, `meta.json`), []byte(`{"name": "001-002"}`), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(block1Code2, `code.sql`), []byte(`SELECT 1; SELECT 2;`), 0644))
	block2 := filepath.Join(blocksDir, `002-block-2`)
	assert.NoError(t, os.MkdirAll(block2, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(block2, `meta.json`), []byte(`{"name": "002"}`), 0644))
	block2Code1 := filepath.Join(block2, `002-code-1`)
	assert.NoError(t, os.MkdirAll(block2Code1, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(block2Code1, `meta.json`), []byte(`{"name": "002-001"}`), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(block2Code1, `code.sql`), []byte(`SELECT 3`), 0644))
	block3 := filepath.Join(blocksDir, `003-block-3`)
	assert.NoError(t, os.MkdirAll(block3, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(block3, `meta.json`), []byte(`{"name": "003"}`), 0644))

	// Load
	logger, _ := utils.NewDebugLogger()
	record, target := createTransTestStructs("keboola.snowflake-transformation")
	assert.NoError(t, LoadBlocks(projectDir, logger, model.DefaultNaming(), record, target))

	// Assert
	expected := `
[
  {
    "name": "001",
    "codes": [
      {
        "name": "001-001",
        "script": [
          "SELECT 1"
        ]
      },
      {
        "name": "001-002",
        "script": [
          "SELECT 1;",
          "SELECT 2;"
        ]
      }
    ]
  },
  {
    "name": "002",
    "codes": [
      {
        "name": "002-001",
        "script": [
          "SELECT 3"
        ]
      }
    ]
  },
  {
    "name": "003",
    "codes": []
  }
]
`
	expected = strings.TrimPrefix(expected, "\n")
	parametersRaw, found := target.Content.Get(`parameters`)
	assert.True(t, found)
	parameters := parametersRaw.(orderedmap.OrderedMap)
	value, found := parameters.Get(`blocks`)
	assert.True(t, found)
	assert.Equal(t, expected, json.MustEncodeString(value, true))
	assert.Equal(t, expected, json.MustEncodeString(target.Blocks, true))
}

func TestLoadTransformationPy(t *testing.T) {
	projectDir := t.TempDir()
	blocksDir := filepath.Join(projectDir, `branch`, `config`, `blocks`)

	// Create files/dirs
	assert.NoError(t, os.MkdirAll(blocksDir, 0755))
	block1 := filepath.Join(blocksDir, `001-block-1`)
	assert.NoError(t, os.MkdirAll(block1, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(block1, `meta.json`), []byte(`{"name": "001"}`), 0644))
	block1Code1 := filepath.Join(block1, `001-code-1`)
	assert.NoError(t, os.MkdirAll(block1Code1, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(block1Code1, `meta.json`), []byte(`{"name": "001-001"}`), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(block1Code1, `code.py`), []byte(`print('1')`), 0644))
	block1Code2 := filepath.Join(blocksDir, `001-block-1`, `002-code-2`)
	assert.NoError(t, os.MkdirAll(block1Code2, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(block1Code2, `meta.json`), []byte(`{"name": "001-002"}`), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(block1Code2, `code.py`), []byte("print('1')\nprint('2')"), 0644))
	block2 := filepath.Join(blocksDir, `002-block-2`)
	assert.NoError(t, os.MkdirAll(block2, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(block2, `meta.json`), []byte(`{"name": "002"}`), 0644))
	block2Code1 := filepath.Join(block2, `002-code-1`)
	assert.NoError(t, os.MkdirAll(block2Code1, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(block2Code1, `meta.json`), []byte(`{"name": "002-001"}`), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(block2Code1, `code.py`), []byte(`print('3')`), 0644))
	block3 := filepath.Join(blocksDir, `003-block-3`)
	assert.NoError(t, os.MkdirAll(block3, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(block3, `meta.json`), []byte(`{"name": "003"}`), 0644))

	// Load
	logger, _ := utils.NewDebugLogger()
	record, target := createTransTestStructs("keboola.python-transformation-v2")
	assert.NoError(t, LoadBlocks(projectDir, logger, model.DefaultNaming(), record, target))

	// Assert
	expected := `
[
  {
    "name": "001",
    "codes": [
      {
        "name": "001-001",
        "script": [
          "print('1')"
        ]
      },
      {
        "name": "001-002",
        "script": [
          "print('1')\nprint('2')"
        ]
      }
    ]
  },
  {
    "name": "002",
    "codes": [
      {
        "name": "002-001",
        "script": [
          "print('3')"
        ]
      }
    ]
  },
  {
    "name": "003",
    "codes": []
  }
]
`
	expected = strings.TrimPrefix(expected, "\n")
	parametersRaw, found := target.Content.Get(`parameters`)
	assert.True(t, found)
	parameters := parametersRaw.(orderedmap.OrderedMap)
	value, found := parameters.Get(`blocks`)
	assert.True(t, found)
	assert.Equal(t, expected, json.MustEncodeString(value, true))
	assert.Equal(t, expected, json.MustEncodeString(target.Blocks, true))
}

func createTransTestStructs(componentId string) (*model.ConfigManifest, *model.Config) {
	configKey := model.ConfigKey{
		ComponentId: componentId,
	}
	record := &model.ConfigManifest{
		ConfigKey: configKey,
		Paths: model.Paths{
			ParentPath: "branch",
			Path:       "config",
		},
	}
	config := &model.Config{
		ConfigKey: configKey,
		Content:   utils.NewOrderedMap(),
	}

	return record, config
}
