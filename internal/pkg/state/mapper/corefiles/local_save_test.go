package corefiles_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestCoreFilesMapper_MapBeforeLocalSave(t *testing.T) {
	t.Parallel()
	state, _ := createStateWithMapper(t)

	// Fixtures
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
	baseDir := model.NewAbsPath("foo", "bar")
	state.NamingRegistry().MustAttach(object.Key(), baseDir)

	// Call mapper
	ctx, err := state.Mapper().MapBeforeLocalSave(context.Background(), object, nil)
	assert.NoError(t, err)

	// Files are generated
	expectedFiles := filesystem.NewFiles()
	expectedFiles.
		Add(
			filesystem.NewJsonFile(state.NamingGenerator().MetaFilePath(baseDir),
				orderedmap.FromPairs([]orderedmap.Pair{
					{Key: "myKey", Value: "3"},
					{Key: "Meta2", Value: "4"},
				}),
			),
		).
		AddTag(local.FileTypeJson).
		AddTag(local.FileKindObjectMeta)
	expectedFiles.
		Add(
			filesystem.NewJsonFile(state.NamingGenerator().ConfigFilePath(baseDir),
				orderedmap.FromPairs([]orderedmap.Pair{
					{Key: "foo", Value: "bar"},
				}),
			),
		).
		AddTag(local.FileTypeJson).
		AddTag(local.FileKindObjectConfig)
	assert.Equal(t, expectedFiles, ctx.ToSave())
}
