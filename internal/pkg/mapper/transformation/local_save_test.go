package transformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/transformation"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestLocalSaveTransformationEmpty(t *testing.T) {
	t.Parallel()
	context, configState := createTestFixtures(t, "keboola.snowflake-transformation")
	recipe := fixtures.NewLocalSaveRecipe(configState.Manifest(), configState.Local)
	fs := context.Fs
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)
	assert.NoError(t, fs.Mkdir(blocksDir))

	// Save
	err := NewMapper(context).MapBeforeLocalSave(recipe)
	assert.NoError(t, err)
	configFile, err := recipe.Files.ObjectConfigFile()
	assert.NoError(t, err)
	assert.Equal(t, "{}\n", json.MustEncodeString(configFile.Content, true))
}

func TestLocalSaveTransformation(t *testing.T) {
	t.Parallel()
	context, configState := createTestFixtures(t, "keboola.snowflake-transformation")
	recipe := fixtures.NewLocalSaveRecipe(configState.Manifest(), configState.Local)
	fs := context.Fs
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)
	assert.NoError(t, fs.Mkdir(blocksDir))

	// Prepare
	configFile, err := recipe.Files.ObjectConfigFile()
	assert.NoError(t, err)
	configFile.Content.Set(`foo`, `bar`)
	configState.Local.Blocks = model.Blocks{
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

	// Minify JSON + remove file description
	var files []*filesystem.File
	for _, fileRaw := range recipe.Files.All() {
		var file *filesystem.File
		if f, ok := fileRaw.File().(*filesystem.JsonFile); ok {
			file = filesystem.NewFile(f.GetPath(), json.MustEncodeString(f.Content, false))
		} else {
			var err error
			file, err = fileRaw.ToFile()
			assert.NoError(t, err)
			file.SetDescription(``)
		}
		files = append(files, file)
	}

	// Check files
	assert.Equal(t, []*filesystem.File{
		filesystem.NewFile(`meta.json`, `{}`),
		filesystem.NewFile(`config.json`, `{"foo":"bar"}`),
		filesystem.NewFile(`description.md`, ``),
		filesystem.NewFile(blocksDir+`/.gitkeep`, ``),
		filesystem.NewFile(blocksDir+`/001-block-1/meta.json`, `{"name":"block1"}`),
		filesystem.NewFile(blocksDir+`/001-block-1/001-code-1/meta.json`, `{"name":"code1"}`),
		filesystem.NewFile(blocksDir+`/001-block-1/001-code-1/code.sql`, "SELECT 1\n"),
		filesystem.NewFile(blocksDir+`/001-block-1/002-code-2/meta.json`, `{"name":"code2"}`),
		filesystem.NewFile(blocksDir+`/001-block-1/002-code-2/code.sql`, "SELECT 2;\n\nSELECT 3;\n"),
		filesystem.NewFile(blocksDir+`/002-block-2/meta.json`, `{"name":"block2"}`),
		filesystem.NewFile(blocksDir+`/002-block-2/001-code-3/meta.json`, `{"name":"code3"}`),
		filesystem.NewFile(blocksDir+`/002-block-2/001-code-3/code.sql`, "\n"),
	}, files)
}
