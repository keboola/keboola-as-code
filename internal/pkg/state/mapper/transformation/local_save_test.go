package transformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestTransformationLocalMapper_MapBeforeLocalSave_Empty(t *testing.T) {
	t.Parallel()
	state, _ := createLocalStateWithMapper(t)

	// Fixtures
	branchKey := model.BranchKey{Id: 123}
	state.MustAdd(&model.Branch{BranchKey: branchKey})
	state.NamingRegistry().MustAttach(branchKey, model.NewAbsPath("", "branch"))
	config, configPath := createTestFixtures(t, "keboola.snowflake-transformation")
	state.MustAdd(config)
	state.NamingRegistry().MustAttach(config.ConfigKey, configPath)

	// Save
	recipe := model.NewLocalSaveRecipe(configPath, config, model.NewChangedFields())
	err := state.Mapper().MapBeforeLocalSave(recipe)
	assert.NoError(t, err)
	assert.NoError(t, err)
	assert.Equal(t, `{"foo":"bar"}`, json.MustEncodeString(config.Content, false))
}

func TestTransformationMapper_MapBeforeLocalSave(t *testing.T) {
	t.Parallel()
	state, d := createLocalStateWithMapper(t)
	logger := d.DebugLogger()

	// Fixtures
	branchKey := model.BranchKey{Id: 123}
	state.MustAdd(&model.Branch{BranchKey: branchKey})
	state.NamingRegistry().MustAttach(branchKey, model.NewAbsPath("", "branch"))
	config, configPath := createTestFixtures(t, "keboola.snowflake-transformation")
	state.MustAdd(config)
	state.NamingRegistry().MustAttach(config.ConfigKey, configPath)
	blocksDir := filesystem.Join(configPath.String(), `blocks`)

	// Prepare
	config.Content.Set(`foo`, `bar`)
	config.Transformation = &model.Transformation{
		Blocks: []*model.Block{
			{
				BlockKey: model.BlockKey{
					BranchId:    123,
					ComponentId: "keboola.snowflake-transformation",
					ConfigId:    `456`,
					Index:       0,
				},
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
						Name:         "code1",
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
						CodeFileName: `code.sql`,
						Name:         "code2",
						Scripts: model.Scripts{
							model.StaticScript{Value: "SELECT 2;"},
							model.StaticScript{Value: "SELECT 3;"},
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
					},
				},
			},
		},
	}

	// Save
	recipe := model.NewLocalSaveRecipe(configPath, config, model.NewChangedFields())
	assert.NoError(t, state.Mapper().MapBeforeLocalSave(recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Minify JSON + remove file description
	var files []filesystem.File
	for _, file := range recipe.Files.All() {
		var fileRaw *filesystem.RawFile
		if f, ok := file.(*filesystem.JsonFile); ok {
			// Minify JSON
			fileRaw = filesystem.NewRawFile(f.Path(), json.MustEncodeString(f.Content, false))
			fileRaw.AddTag(f.AllTags()...)
		} else {
			var err error
			fileRaw, err = file.ToRawFile()
			assert.NoError(t, err)
			fileRaw.SetDescription(``)
		}
		files = append(files, fileRaw)
	}

	// Check files
	assert.Equal(t, []filesystem.File{
		filesystem.
			NewRawFile(blocksDir+`/.gitkeep`, ``).
			AddTag(model.FileKindGitKeep).
			AddTag(model.FileTypeOther),
		filesystem.NewRawFile(blocksDir+`/001-block-1/meta.json`, `{"name":"block1"}`).
			AddTag(model.FileKindBlockMeta).
			AddTag(model.FileTypeJson),
		filesystem.NewRawFile(blocksDir+`/001-block-1/001-code-1/meta.json`, `{"name":"code1"}`).
			AddTag(model.FileKindCodeMeta).
			AddTag(model.FileTypeJson),
		filesystem.NewRawFile(blocksDir+`/001-block-1/001-code-1/code.sql`, "SELECT 1\n").
			AddTag(model.FileKindNativeCode).
			AddTag(model.FileTypeOther),
		filesystem.NewRawFile(blocksDir+`/001-block-1/002-code-2/meta.json`, `{"name":"code2"}`).
			AddTag(model.FileKindCodeMeta).
			AddTag(model.FileTypeJson),
		filesystem.NewRawFile(blocksDir+`/001-block-1/002-code-2/code.sql`, "SELECT 2;\n\nSELECT 3;\n").
			AddTag(model.FileKindNativeCode).
			AddTag(model.FileTypeOther),
		filesystem.NewRawFile(blocksDir+`/002-block-2/meta.json`, `{"name":"block2"}`).
			AddTag(model.FileKindBlockMeta).
			AddTag(model.FileTypeJson),
		filesystem.NewRawFile(blocksDir+`/002-block-2/001-code-3/meta.json`, `{"name":"code3"}`).
			AddTag(model.FileKindCodeMeta).
			AddTag(model.FileTypeJson),
		filesystem.NewRawFile(blocksDir+`/002-block-2/001-code-3/code.sql`, "\n").
			AddTag(model.FileKindNativeCode).
			AddTag(model.FileTypeOther),
		filesystem.NewRawFile(configPath.String()+`/meta.json`, `{"name":"My Config"}`).
			AddTag(model.FileKindObjectMeta).
			AddTag(model.FileTypeJson),
		filesystem.NewRawFile(configPath.String()+`/config.json`, `{"foo":"bar"}`).
			AddTag(model.FileKindObjectConfig).
			AddTag(model.FileTypeJson),
		filesystem.NewRawFile(configPath.String()+`/description.md`, "\n").
			AddTag(model.FileKindObjectDescription).
			AddTag(model.FileTypeMarkdown),
	}, files)
}
