package local

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestSaveContext(t *testing.T) {
	t.Parallel()
	ctx, err := NewSaveContext(context.Background(), nil, nil, model.NewChangedFields())
	assert.NoError(t, err)

	// Add file
	fileRaw1 := filesystem.NewRawFile(`path1`, `content1`)
	file1 := ctx.Add(fileRaw1)
	assert.Same(t, fileRaw1, file1)
	result, err := fileRaw1.ToRawFile()
	assert.NoError(t, err)
	assert.Same(t, fileRaw1, result)
	assert.Equal(t, fileRaw1.Path(), file1.Path())

	// Add tag 1
	tag1 := `my-tag-1`
	assert.False(t, file1.HasTag(tag1))
	assert.Empty(t, ctx.GetByTag(tag1))
	assert.Nil(t, ctx.GetOneByTag(tag1))
	file1.AddTag(tag1)
	assert.True(t, file1.HasTag(tag1))
	assert.Equal(t, []filesystem.File{file1}, ctx.GetByTag(tag1))
	assert.Equal(t, file1, ctx.GetOneByTag(tag1))

	// Add tag 2
	tag2 := `my-tag-2`
	assert.False(t, file1.HasTag(tag2))
	assert.Empty(t, ctx.GetByTag(tag2))
	assert.Nil(t, ctx.GetOneByTag(tag2))
	file1.AddTag(tag2)
	assert.True(t, file1.HasTag(tag2))
	assert.Equal(t, []filesystem.File{file1}, ctx.GetByTag(tag2))
	assert.Equal(t, file1, ctx.GetOneByTag(tag2))

	// Two files with same tag
	fileRaw2 := filesystem.NewRawFile(`path2`, `content2`)
	file2 := ctx.Add(fileRaw2)
	assert.False(t, file2.HasTag(tag2))
	file2.AddTag(tag2)
	assert.True(t, file2.HasTag(tag2))
	assert.Equal(t, []filesystem.File{file1, file2}, ctx.GetByTag(tag2))
	assert.PanicsWithError(t, `found multiple files with tag "my-tag-2": "path1", "path2"`, func() {
		ctx.GetOneByTag(tag2)
	})

	// Delete tag which is not set
	file1.RemoveTag(`my-tag-3`)

	// Delete tag which is not set
	file1.RemoveTag(tag2)
	assert.False(t, file1.HasTag(tag2))
	assert.True(t, file2.HasTag(tag2))
	assert.Equal(t, []filesystem.File{file2}, ctx.GetByTag(tag2))
	assert.Equal(t, file2, ctx.GetOneByTag(tag2))

	// All
	assert.Equal(t, []filesystem.File{file1, file2}, ctx.All())
}

type myStruct struct {
	Field1   string                 `json:"field1" mytag:"field"`
	Field2   string                 `json:"field2" mytag:"field"`
	FooField string                 `json:"foo"`
	Map      *orderedmap.OrderedMap `mytag:"map"`
	Content  string                 `mytag:"content"`
}
