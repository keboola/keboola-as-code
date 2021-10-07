package transformation

import (
	"strings"
	"testing"

	"github.com/iancoleman/orderedmap"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestLoadTransformationMissingBlockMetaSql(t *testing.T) {
	logger, fs, state, objectFiles := createTestFixtures(t)
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)

	// Create files/dirs
	assert.NoError(t, fs.Mkdir(blocksDir))
	block1 := filesystem.Join(blocksDir, `001-block-1`)
	assert.NoError(t, fs.Mkdir(block1))

	// Load, assert
	err := Load(logger, fs, model.DefaultNaming(), state, objectFiles)
	assert.Error(t, err)
	assert.Equal(t, `missing block metadata file "branch/config/blocks/001-block-1/meta.json"`, err.Error())
}

func TestLoadTransformationMissingCodeMeta(t *testing.T) {
	logger, fs, state, objectFiles := createTestFixtures(t)
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)

	// Create files/dirs
	assert.NoError(t, fs.Mkdir(blocksDir))
	block1 := filesystem.Join(blocksDir, `001-block-1`)
	assert.NoError(t, fs.Mkdir(block1))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(block1, `meta.json`), `{"name": "001"}`)))
	block1Code1 := filesystem.Join(block1, `001-code-1`)
	assert.NoError(t, fs.Mkdir(block1Code1))

	// Load, assert
	err := Load(logger, fs, model.DefaultNaming(), state, objectFiles)
	assert.Error(t, err)
	assert.Equal(t, strings.Join([]string{
		`- missing code metadata file "branch/config/blocks/001-block-1/001-code-1/meta.json"`,
		`- missing code file in "branch/config/blocks/001-block-1/001-code-1"`,
	}, "\n"), err.Error())
}

func TestLoadTransformationSql(t *testing.T) {
	logger, fs, state, objectFiles := createTestFixtures(t)
	config := objectFiles.Object.(*model.Config)
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)

	// Create files/dirs
	assert.NoError(t, fs.Mkdir(blocksDir))
	block1 := filesystem.Join(blocksDir, `001-block-1`)
	assert.NoError(t, fs.Mkdir(block1))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(block1, `meta.json`), `{"name": "001"}`)))
	block1Code1 := filesystem.Join(block1, `001-code-1`)
	assert.NoError(t, fs.Mkdir(block1Code1))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(block1Code1, `meta.json`), `{"name": "001-001"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(block1Code1, `code.sql`), `SELECT 1`)))
	block1Code2 := filesystem.Join(blocksDir, `001-block-1`, `002-code-2`)
	assert.NoError(t, fs.Mkdir(block1Code2))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(block1Code2, `meta.json`), `{"name": "001-002"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(block1Code2, `code.sql`), `SELECT 1; SELECT 2;`)))
	block2 := filesystem.Join(blocksDir, `002-block-2`)
	assert.NoError(t, fs.Mkdir(block2))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(block2, `meta.json`), `{"name": "002"}`)))
	block2Code1 := filesystem.Join(block2, `002-code-1`)
	assert.NoError(t, fs.Mkdir(block2Code1))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(block2Code1, `meta.json`), `{"name": "002-001"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(block2Code1, `code.sql`), `SELECT 3`)))
	block3 := filesystem.Join(blocksDir, `003-block-3`)
	assert.NoError(t, fs.Mkdir(block3))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(block3, `meta.json`), `{"name": "003"}`)))

	// Load
	assert.NoError(t, Load(logger, fs, model.DefaultNaming(), state, objectFiles))

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
	parametersRaw, found := config.Content.Get(`parameters`)
	assert.True(t, found)
	parameters := parametersRaw.(orderedmap.OrderedMap)
	value, found := parameters.Get(`blocks`)
	assert.True(t, found)
	assert.Equal(t, expected, json.MustEncodeString(value, true))
	assert.Equal(t, expected, json.MustEncodeString(config.Blocks, true))
}

func TestLoadTransformationPy(t *testing.T) {
	logger, fs, state, objectFiles := createTestFixtures(t)
	config := objectFiles.Object.(*model.Config)
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)

	// Create files/dirs
	assert.NoError(t, fs.Mkdir(blocksDir))
	block1 := filesystem.Join(blocksDir, `001-block-1`)
	assert.NoError(t, fs.Mkdir(block1))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(block1, `meta.json`), `{"name": "001"}`)))
	block1Code1 := filesystem.Join(block1, `001-code-1`)
	assert.NoError(t, fs.Mkdir(block1Code1))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(block1Code1, `meta.json`), `{"name": "001-001"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(block1Code1, `code.py`), `print('1')`)))
	block1Code2 := filesystem.Join(blocksDir, `001-block-1`, `002-code-2`)
	assert.NoError(t, fs.Mkdir(block1Code2))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(block1Code2, `meta.json`), `{"name": "001-002"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(block1Code2, `code.py`), "print('1')\nprint('2')")))
	block2 := filesystem.Join(blocksDir, `002-block-2`)
	assert.NoError(t, fs.Mkdir(block2))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(block2, `meta.json`), `{"name": "002"}`)))
	block2Code1 := filesystem.Join(block2, `002-code-1`)
	assert.NoError(t, fs.Mkdir(block2Code1))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(block2Code1, `meta.json`), `{"name": "002-001"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(block2Code1, `code.py`), `print('3')`)))
	block3 := filesystem.Join(blocksDir, `003-block-3`)
	assert.NoError(t, fs.Mkdir(block3))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(block3, `meta.json`), `{"name": "003"}`)))

	// Load
	assert.NoError(t, Load(logger, fs, model.DefaultNaming(), state, objectFiles))

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
	parametersRaw, found := config.Content.Get(`parameters`)
	assert.True(t, found)
	parameters := parametersRaw.(orderedmap.OrderedMap)
	value, found := parameters.Get(`blocks`)
	assert.True(t, found)
	assert.Equal(t, expected, json.MustEncodeString(value, true))
	assert.Equal(t, expected, json.MustEncodeString(config.Blocks, true))
}

func createTestFixtures(t *testing.T) (*zap.SugaredLogger, filesystem.Fs, *model.State, *model.ObjectFiles) {
	t.Helper()

	configKey := model.ConfigKey{
		ComponentId: "keboola.snowflake-transformation",
	}

	record := &model.ConfigManifest{
		ConfigKey: configKey,
		Paths: model.Paths{
			PathInProject: model.PathInProject{
				ParentPath: "branch",
				ObjectPath: "config",
			},
		},
	}

	config := &model.Config{
		ConfigKey: configKey,
		Content:   utils.NewOrderedMap(),
	}

	objectFiles := &model.ObjectFiles{
		Object:        config,
		Record:        record,
		Metadata:      filesystem.CreateJsonFile(model.MetaFile, utils.NewOrderedMap()),
		Configuration: filesystem.CreateJsonFile(model.ConfigFile, utils.NewOrderedMap()),
		Description:   filesystem.CreateFile(model.DescriptionFile, ``),
	}

	logger, _ := utils.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, ".")
	assert.NoError(t, err)

	state := model.NewState(zap.NewNop().Sugar(), fs, model.NewComponentsMap(nil), model.SortByPath)
	return logger, fs, state, objectFiles
}
