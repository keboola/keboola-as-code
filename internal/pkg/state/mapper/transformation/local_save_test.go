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
	config, configPath := createTestFixtures(t, "keboola.snowflake-transformation", state)

	// Save
	recipe := model.NewLocalSaveRecipe(configPath, config, model.NewChangedFields())
	err := state.Mapper().MapBeforeLocalSave(recipe)
	assert.NoError(t, err)
	assert.Equal(t, `{"parameters":{"foo":"bar"}}`, json.MustEncodeString(config.Content, false))
}

func TestTransformationLocalMapper_MapBeforeLocalSave(t *testing.T) {
	t.Parallel()
	state, d := createLocalStateWithMapper(t)
	logger := d.DebugLogger()

	// Fixtures
	config, configPath := createTestFixtures(t, "keboola.snowflake-transformation", state)
	state.MustAdd(config)
	state.NamingRegistry().MustAttach(config.ConfigKey, configPath)
	blocksDir := filesystem.Join(configPath.String(), `blocks`)

	// Prepare
	config.Transformation = &model.Transformation{
		Blocks: []*model.Block{
			{
				BlockKey: model.BlockKey{Parent: config.ConfigKey, Index: 0},
				Name:     "block1",
				Codes: model.Codes{
					{
						CodeKey: model.CodeKey{Parent: model.BlockKey{Parent: config.ConfigKey, Index: 0}, Index: 0},
						Name:    "code1",
						Scripts: model.Scripts{
							model.StaticScript{Value: "SELECT 1"},
						},
					},
					{
						CodeKey: model.CodeKey{Parent: model.BlockKey{Parent: config.ConfigKey, Index: 0}, Index: 1},
						Name:    "code2",
						Scripts: model.Scripts{
							model.StaticScript{Value: "SELECT 2;"},
							model.StaticScript{Value: "SELECT 3;"},
						},
					},
				},
			},
			{
				BlockKey: model.BlockKey{Parent: config.ConfigKey, Index: 1},
				Name:     "block2",
				Codes: model.Codes{
					{
						CodeKey: model.CodeKey{Parent: model.BlockKey{Parent: config.ConfigKey, Index: 1}, Index: 0},
						Name:    "code3",
					},
				},
			},
		},
	}

	// Save
	recipe := model.NewLocalSaveRecipe(configPath, config, model.NewChangedFields())
	assert.NoError(t, state.Mapper().MapBeforeLocalSave(recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Config content has not changed
	assert.Equal(t, `{"parameters":{"foo":"bar"}}`, json.MustEncodeString(config.Content, false))

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
		filesystem.NewRawFile(blocksDir+`/001-block1/meta.json`, `{"name":"block1"}`).
			AddTag(model.FileKindBlockMeta).
			AddTag(model.FileTypeJson),
		filesystem.NewRawFile(blocksDir+`/001-block1/001-code1/meta.json`, `{"name":"code1"}`).
			AddTag(model.FileKindCodeMeta).
			AddTag(model.FileTypeJson),
		filesystem.NewRawFile(blocksDir+`/001-block1/001-code1/code.sql`, "SELECT 1\n").
			AddTag(model.FileKindNativeCode).
			AddTag(model.FileTypeOther),
		filesystem.NewRawFile(blocksDir+`/001-block1/002-code2/meta.json`, `{"name":"code2"}`).
			AddTag(model.FileKindCodeMeta).
			AddTag(model.FileTypeJson),
		filesystem.NewRawFile(blocksDir+`/001-block1/002-code2/code.sql`, "SELECT 2;\n\nSELECT 3;\n").
			AddTag(model.FileKindNativeCode).
			AddTag(model.FileTypeOther),
		filesystem.NewRawFile(blocksDir+`/002-block2/meta.json`, `{"name":"block2"}`).
			AddTag(model.FileKindBlockMeta).
			AddTag(model.FileTypeJson),
		filesystem.NewRawFile(blocksDir+`/002-block2/001-code3/meta.json`, `{"name":"code3"}`).
			AddTag(model.FileKindCodeMeta).
			AddTag(model.FileTypeJson),
		filesystem.NewRawFile(blocksDir+`/002-block2/001-code3/code.sql`, "\n").
			AddTag(model.FileKindNativeCode).
			AddTag(model.FileTypeOther),
	}, files)
}
