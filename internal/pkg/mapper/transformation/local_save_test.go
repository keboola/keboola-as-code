package transformation_test

import (
	"context"
	"testing"

	"github.com/keboola/go-utils/pkg/deepcopy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/corefiles"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/transformation"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

func TestLocalSaveTransformationEmpty(t *testing.T) {
	t.Parallel()
	d := dependencies.NewMocked(t, t.Context())
	state := d.MockedState()
	state.Mapper().AddMapper(corefiles.NewMapper(state))
	state.Mapper().AddMapper(transformation.NewMapper(state))
	fs := state.ObjectsRoot()

	configState := createTestFixtures(t, "keboola.snowflake-transformation")
	object := deepcopy.Copy(configState.Local).(*model.Config)
	recipe := model.NewLocalSaveRecipe(configState.Manifest(), object, model.NewChangedFields())

	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)
	require.NoError(t, fs.Mkdir(t.Context(), blocksDir))

	// Save
	err := state.Mapper().MapBeforeLocalSave(t.Context(), recipe)
	require.NoError(t, err)
	require.NoError(t, err)
	assert.JSONEq(t, `{"foo":"bar"}`, json.MustEncodeString(object.Content, false))
}

func TestTransformationMapper_MapBeforeLocalSave(t *testing.T) {
	t.Parallel()
	d := dependencies.NewMocked(t, t.Context())
	state := d.MockedState()
	state.Mapper().AddMapper(corefiles.NewMapper(state))
	state.Mapper().AddMapper(transformation.NewMapper(state))
	fs := state.ObjectsRoot()
	logger := d.DebugLogger()

	configState := createTestFixtures(t, "keboola.snowflake-transformation")
	object := deepcopy.Copy(configState.Local).(*model.Config)
	recipe := model.NewLocalSaveRecipe(configState.Manifest(), configState.Local, model.NewChangedFields())

	configDir := filesystem.Join(`branch`, `config`)
	blocksDir := filesystem.Join(configDir, `blocks`)
	require.NoError(t, fs.Mkdir(t.Context(), blocksDir))

	// Prepare
	object.Content.Set(`foo`, `bar`)
	configState.Local.Transformation = &model.Transformation{
		Blocks: []*model.Block{
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
				Name: "block1",
				Codes: model.Codes{
					{
						CodeKey: model.CodeKey{
							BranchID:    123,
							ComponentID: "keboola.snowflake-transformation",
							ConfigID:    `456`,
							BlockIndex:  0,
							Index:       0,
						},
						CodeFileName: `code.sql`,
						AbsPath: model.NewAbsPath(
							`branch/config/blocks/001-block-1`,
							`001-code-1`,
						),
						Name: "code1",
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
						CodeFileName: `code.sql`,
						AbsPath: model.NewAbsPath(
							`branch/config/blocks/001-block-1`,
							`002-code-2`,
						),
						Name: "code2",
						Scripts: model.Scripts{
							model.StaticScript{Value: "SELECT 2;"},
							model.StaticScript{Value: "SELECT 3;"},
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
				Name: "block2",
				Codes: model.Codes{
					{
						CodeKey: model.CodeKey{
							BranchID:    123,
							ComponentID: "keboola.snowflake-transformation",
							ConfigID:    `456`,
							BlockIndex:  1,
							Index:       0,
						},
						Name:         "code3",
						CodeFileName: `code.sql`,
						AbsPath: model.NewAbsPath(
							`branch/config/blocks/002-block-2`,
							`001-code-3`,
						),
					},
				},
			},
		},
	}

	// Save
	require.NoError(t, state.Mapper().MapBeforeLocalSave(t.Context(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Minify JSON + remove file description
	files := make([]filesystem.File, 0, len(recipe.Files.All()))
	for _, file := range recipe.Files.All() {
		var fileRaw *filesystem.RawFile
		if f, ok := file.(*filesystem.JSONFile); ok {
			// Minify JSON
			fileRaw = filesystem.NewRawFile(f.Path(), json.MustEncodeString(f.Content, false))
			fileRaw.AddTag(f.AllTags()...)
		} else {
			var err error
			fileRaw, err = file.ToRawFile()
			require.NoError(t, err)
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
			AddTag(model.FileTypeJSON),
		filesystem.NewRawFile(blocksDir+`/001-block-1/001-code-1/meta.json`, `{"name":"code1"}`).
			AddTag(model.FileKindCodeMeta).
			AddTag(model.FileTypeJSON),
		filesystem.NewRawFile(blocksDir+`/001-block-1/001-code-1/code.sql`, "SELECT 1\n").
			AddTag(model.FileKindNativeCode).
			AddTag(model.FileTypeOther),
		filesystem.NewRawFile(blocksDir+`/001-block-1/002-code-2/meta.json`, `{"name":"code2"}`).
			AddTag(model.FileKindCodeMeta).
			AddTag(model.FileTypeJSON),
		filesystem.NewRawFile(blocksDir+`/001-block-1/002-code-2/code.sql`, "SELECT 2;\n\nSELECT 3;\n").
			AddTag(model.FileKindNativeCode).
			AddTag(model.FileTypeOther),
		filesystem.NewRawFile(blocksDir+`/002-block-2/meta.json`, `{"name":"block2"}`).
			AddTag(model.FileKindBlockMeta).
			AddTag(model.FileTypeJSON),
		filesystem.NewRawFile(blocksDir+`/002-block-2/001-code-3/meta.json`, `{"name":"code3"}`).
			AddTag(model.FileKindCodeMeta).
			AddTag(model.FileTypeJSON),
		filesystem.NewRawFile(blocksDir+`/002-block-2/001-code-3/code.sql`, "\n").
			AddTag(model.FileKindNativeCode).
			AddTag(model.FileTypeOther),
		filesystem.NewRawFile(configDir+`/meta.json`, `{"name":"My Config","isDisabled":false}`).
			AddTag(model.FileKindObjectMeta).
			AddTag(model.FileTypeJSON),
		filesystem.NewRawFile(configDir+`/config.json`, `{"foo":"bar"}`).
			AddTag(model.FileKindObjectConfig).
			AddTag(model.FileTypeJSON),
		filesystem.NewRawFile(configDir+`/description.md`, "\n").
			AddTag(model.FileKindObjectDescription).
			AddTag(model.FileTypeMarkdown),
	}, files)
}
