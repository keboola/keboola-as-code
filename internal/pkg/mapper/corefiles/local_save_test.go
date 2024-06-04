package corefiles_test

import (
	"context"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSaveCoreFiles(t *testing.T) {
	t.Parallel()
	state := createStateWithMapper(t)

	// Recipe
	manifest := &fixtures.MockedManifest{}
	object := &fixtures.MockedObject{
		Foo1:  "1",
		Foo2:  "2",
		Meta1: "3",
		Meta2: "4",
		Config: orderedmap.FromPairs([]orderedmap.Pair{
			{
				Key:   "foo",
				Value: "bar",
			},
		}),
	}
	recipe := model.NewLocalSaveRecipe(manifest, object, model.NewChangedFields())

	// No files
	assert.Empty(t, recipe.Files.All())

	// Call mapper
	require.NoError(t, state.Mapper().MapBeforeLocalSave(context.Background(), recipe))

	// Files are generated
	expectedFiles := model.NewFilesToSave()
	expectedFiles.
		Add(
			filesystem.NewJSONFile(state.NamingGenerator().MetaFilePath(manifest.Path()),
				orderedmap.FromPairs([]orderedmap.Pair{
					{Key: "myKey", Value: "3"},
					{Key: "Meta2", Value: "4"},
				}),
			),
		).
		AddTag(model.FileTypeJSON).
		AddTag(model.FileKindObjectMeta)
	expectedFiles.
		Add(
			filesystem.NewJSONFile(state.NamingGenerator().ConfigFilePath(manifest.Path()),
				orderedmap.FromPairs([]orderedmap.Pair{
					{Key: "foo", Value: "bar"},
				}),
			),
		).
		AddTag(model.FileTypeJSON).
		AddTag(model.FileKindObjectConfig)
	assert.Equal(t, expectedFiles, recipe.Files)
}
