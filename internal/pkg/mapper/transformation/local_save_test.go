package transformation_test

import (
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
	require.NoError(t, fs.Mkdir(t.Context(), configDir))

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

	// Expected content for the new developer-friendly format (single transform.sql file)
	// Note: block2/code3 has empty content so only block1 is included
	expectedTransformContent := `/* ===== BLOCK: block1 ===== */

/* ===== CODE: code1 ===== */
SELECT 1;

/* ===== CODE: code2 ===== */
SELECT 2;
SELECT 3;`

	// Expected _config.yml content
	expectedConfigYAML := `version: 2
name: My Config
_keboola:
    component_id: keboola.snowflake-transformation
    config_id: "456"
`

	// Check files - now using developer-friendly format with single transform.sql and unified _config.yml
	assert.Equal(t, []filesystem.File{
		filesystem.NewRawFile(configDir+`/transform.sql`, expectedTransformContent).
			AddTag(model.FileKindNativeCode).
			AddTag(model.FileTypeOther),
		filesystem.NewRawFile(configDir+`/_config.yml`, expectedConfigYAML).
			AddTag(model.FileKindObjectConfig).
			AddTag(model.FileTypeYaml),
	}, files)
}
