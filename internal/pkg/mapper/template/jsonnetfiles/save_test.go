package jsonnetfiles_test

import (
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestJsonnetMapper_MapBeforeLocalSave(t *testing.T) {
	t.Parallel()
	state := createStateWithMapper(t, nil)

	manifest := &model.ConfigManifest{}
	object := &model.Config{}
	recipe := model.NewLocalSaveRecipe(manifest, object, model.NewChangedFields())

	// Some Json and markdown file
	jsonContent := orderedmap.FromPairs([]orderedmap.Pair{{Key: "key", Value: "value"}})
	recipe.Files.
		Add(filesystem.NewJSONFile(`foo.json`, jsonContent)).
		AddTag(model.FileTypeJSON)
	recipe.Files.
		Add(filesystem.NewRawFile(`README.md`, `content`)).
		AddTag(model.FileTypeMarkdown)

	// Run mapper
	require.NoError(t, state.Mapper().MapBeforeLocalSave(t.Context(), recipe))

	// Json file is converted to Jsonnet
	expectedAst, err := jsonnet.ToAst("{\n  \"key\": \"value\"\n}\n", "foo.jsonnet")
	require.NoError(t, err)
	expected := model.NewFilesToSave()
	expected.
		Add(filesystem.NewJsonnetFile(`foo.jsonnet`, expectedAst, nil)). // <<<<<<<
		AddTag(model.FileTypeJsonnet)
	expected.
		Add(filesystem.NewRawFile(`README.md`, `content`)).
		AddTag(model.FileTypeMarkdown)
	assert.Equal(t, expected, recipe.Files)

	// Jsonnet file content
	f, err := recipe.Files.GetOneByTag(model.FileTypeJsonnet).ToRawFile()
	require.NoError(t, err)
	assert.Equal(t, "{\n  key: \"value\",\n}\n", f.Content)
}
