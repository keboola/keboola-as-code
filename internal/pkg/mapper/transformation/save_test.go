package transformation_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/transformation"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestRemoteSaveTransformation(t *testing.T) {
	t.Parallel()
	context, internalConfig, configRecord := createTestFixtures(t, "keboola.snowflake-transformation")
	blocks := model.Blocks{
		{
			Name: "001",
			Codes: model.Codes{
				{
					Name: "001-001",
					Scripts: []string{
						"SELECT 1",
					},
				},
				{
					Name: "001-002",
					Scripts: []string{
						"SELECT 1;",
						"SELECT 2;",
					},
				},
			},
		},
		{
			Name: "002",
			Codes: model.Codes{
				{
					Name: "002-001",
					Scripts: []string{
						"SELECT 3",
					},
				},
			},
		},
		{
			Name:  "003",
			Codes: model.Codes{},
		},
	}
	internalConfig.Blocks = blocks
	apiConfig := internalConfig.Clone().(*model.Config)
	recipe := &model.RemoteSaveRecipe{
		ChangedFields:  model.NewChangedFields("blocks"),
		Manifest:       configRecord,
		InternalObject: internalConfig,
		ApiObject:      apiConfig,
	}

	// Save
	assert.NoError(t, NewMapper(context).MapBeforeRemoteSave(recipe))

	// Internal object is not modified
	assert.NotEmpty(t, internalConfig.Blocks)
	assert.Nil(t, utils.GetFromMap(internalConfig.Content, []string{`parameters`, `blocks`}))

	// Blocks are stored in API object content
	expectedBlocks := `
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
	assert.Empty(t, apiConfig.Blocks)
	apiBlocks := utils.GetFromMap(apiConfig.Content, []string{`parameters`, `blocks`})
	assert.NotNil(t, blocks)
	assert.Equal(t, strings.TrimLeft(expectedBlocks, "\n"), json.MustEncodeString(apiBlocks, true))

	// Check changed fields
	assert.Equal(t, `configuration`, recipe.ChangedFields.String())
}

func TestLocalSaveTransformationEmpty(t *testing.T) {
	t.Parallel()
	context, config, configRecord := createTestFixtures(t, "keboola.snowflake-transformation")
	recipe := createLocalSaveRecipe(config, configRecord)
	fs := context.Fs
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)
	assert.NoError(t, fs.Mkdir(blocksDir))

	// Save
	err := NewMapper(context).MapBeforeLocalSave(recipe)
	assert.NoError(t, err)
	configContent := json.MustEncodeString(recipe.Configuration.Content, false)
	assert.Equal(t, `{}`, configContent)
}

func TestLocalSaveTransformation(t *testing.T) {
	t.Parallel()
	context, config, configRecord := createTestFixtures(t, "keboola.snowflake-transformation")
	recipe := createLocalSaveRecipe(config, configRecord)
	fs := context.Fs
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)
	assert.NoError(t, fs.Mkdir(blocksDir))

	// Prepare
	recipe.Configuration.Content.Set(`foo`, `bar`)
	config.Blocks = model.Blocks{
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
			Name: "block1",
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{
						BranchId:    123,
						ComponentId: "keboola.snowflake-transformation",
						ConfigId:    `456`,
						BlockIndex:  0,
						Index:       0,
					},
					CodeFileName: `code.sql`,
					PathInProject: model.NewPathInProject(
						`branch/config/blocks/001-block-1`,
						`001-code-1`,
					),
					Name: "code1",
					Scripts: []string{
						"SELECT 1",
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
					CodeFileName: `code.sql`,
					PathInProject: model.NewPathInProject(
						`branch/config/blocks/001-block-1`,
						`002-code-2`,
					),
					Name: "code2",
					Scripts: []string{
						"SELECT 2;",
						"SELECT 3;",
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
			Name: "block2",
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{
						BranchId:    123,
						ComponentId: "keboola.snowflake-transformation",
						ConfigId:    `456`,
						BlockIndex:  1,
						Index:       0,
					},
					Name:         "code3",
					CodeFileName: `code.sql`,
					PathInProject: model.NewPathInProject(
						`branch/config/blocks/002-block-2`,
						`001-code-3`,
					),
				},
			},
		},
	}

	// Save
	assert.NoError(t, NewMapper(context).MapBeforeLocalSave(recipe))

	// Blocks are not part of the config.json
	configContent := json.MustEncodeString(recipe.Configuration.Content, false)
	assert.Equal(t, `{"foo":"bar"}`, configContent)

	// Check generated files
	assert.Equal(t, []*filesystem.File{
		filesystem.CreateFile(blocksDir+`/.gitkeep`, ``),
		filesystem.CreateFile(blocksDir+`/001-block-1/meta.json`, "{\n  \"name\": \"block1\"\n}\n").SetDescription(`block metadata`),
		filesystem.CreateFile(blocksDir+`/001-block-1/001-code-1/meta.json`, "{\n  \"name\": \"code1\"\n}\n").SetDescription(`code metadata`),
		filesystem.CreateFile(blocksDir+`/001-block-1/001-code-1/code.sql`, "SELECT 1\n").SetDescription(`code`),
		filesystem.CreateFile(blocksDir+`/001-block-1/002-code-2/meta.json`, "{\n  \"name\": \"code2\"\n}\n").SetDescription(`code metadata`),
		filesystem.CreateFile(blocksDir+`/001-block-1/002-code-2/code.sql`, "SELECT 2;\n\nSELECT 3;\n").SetDescription(`code`),
		filesystem.CreateFile(blocksDir+`/002-block-2/meta.json`, "{\n  \"name\": \"block2\"\n}\n").SetDescription(`block metadata`),
		filesystem.CreateFile(blocksDir+`/002-block-2/001-code-3/meta.json`, "{\n  \"name\": \"code3\"\n}\n").SetDescription(`code metadata`),
		filesystem.CreateFile(blocksDir+`/002-block-2/001-code-3/code.sql`, "\n").SetDescription(`code`),
	}, recipe.ExtraFiles)
}
