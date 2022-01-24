package jsonnetfiles_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestJsonNetMapper_MapBeforeLocalSave(t *testing.T) {
	t.Parallel()
	state := createStateWithMapper(t, nil)

	manifest := &model.ConfigManifest{}
	object := &model.Config{}
	recipe := model.NewLocalSaveRecipe(manifest, object, model.NewChangedFields())

	// Some Json and markdown file
	jsonContent := orderedmap.FromPairs([]orderedmap.Pair{{Key: "key", Value: "value"}})
	recipe.Files.
		Add(filesystem.NewJsonFile(`foo.json`, jsonContent)).
		AddTag(model.FileTypeJson)
	recipe.Files.
		Add(filesystem.NewRawFile(`README.md`, `content`)).
		AddTag(model.FileTypeMarkdown)

	// Run mapper
	assert.NoError(t, state.Mapper().MapBeforeLocalSave(recipe))

	// Json file is converted to JsonNet
	expectedAst, err := jsonnet.ToAst("{\n  \"key\": \"value\"\n}\n")
	assert.NoError(t, err)
	expected := model.NewFilesToSave()
	expected.
		Add(filesystem.NewJsonNetFile(`foo.jsonnet`, expectedAst)). // <<<<<<<
		AddTag(model.FileTypeJsonNet)
	expected.
		Add(filesystem.NewRawFile(`README.md`, `content`)).
		AddTag(model.FileTypeMarkdown)
	assert.Equal(t, expected, recipe.Files)

	// JsonNet file content
	f, err := recipe.Files.GetOneByTag(model.FileTypeJsonNet).ToRawFile()
	assert.NoError(t, err)
	assert.Equal(t, "{\n  key: \"value\",\n}\n", f.Content)
}
