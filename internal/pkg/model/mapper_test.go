package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

func TestObjectFiles(t *testing.T) {
	t.Parallel()
	files := ObjectFiles{}

	// Add file
	fileRaw1 := filesystem.NewFile(`path1`, `content1`)
	file1 := files.Add(fileRaw1)
	assert.Same(t, fileRaw1, file1.File())
	result, err := fileRaw1.ToFile()
	assert.NoError(t, err)
	assert.Same(t, fileRaw1, result)
	assert.Equal(t, fileRaw1.GetPath(), file1.Path())

	// Add tag 1
	tag1 := `my-tag-1`
	assert.False(t, file1.HasTag(tag1))
	assert.Empty(t, files.GetByTag(tag1))
	assert.Nil(t, files.GetOneByTag(tag1))
	file1.AddTag(tag1)
	assert.True(t, file1.HasTag(tag1))
	assert.Equal(t, []*objectFile{file1}, files.GetByTag(tag1))
	assert.Equal(t, file1, files.GetOneByTag(tag1))

	// Add tag 2
	tag2 := `my-tag-2`
	assert.False(t, file1.HasTag(tag2))
	assert.Empty(t, files.GetByTag(tag2))
	assert.Nil(t, files.GetOneByTag(tag2))
	file1.AddTag(tag2)
	assert.True(t, file1.HasTag(tag2))
	assert.Equal(t, []*objectFile{file1}, files.GetByTag(tag2))
	assert.Equal(t, file1, files.GetOneByTag(tag2))

	// Two files with same tag
	fileRaw2 := filesystem.NewFile(`path2`, `content2`)
	file2 := files.Add(fileRaw2)
	assert.False(t, file2.HasTag(tag2))
	file2.AddTag(tag2)
	assert.True(t, file2.HasTag(tag2))
	assert.Equal(t, []*objectFile{file1, file2}, files.GetByTag(tag2))
	assert.PanicsWithError(t, `found multiple files with tag "my-tag-2": "path1", "path2"`, func() {
		files.GetOneByTag(tag2)
	})

	// Delete tag which is not set
	file1.DeleteTag(`my-tag-3`)

	// Delete tag which is not set
	file1.DeleteTag(tag2)
	assert.False(t, file1.HasTag(tag2))
	assert.True(t, file2.HasTag(tag2))
	assert.Equal(t, []*objectFile{file2}, files.GetByTag(tag2))
	assert.Equal(t, file2, files.GetOneByTag(tag2))

	// All
	assert.Equal(t, []*objectFile{file1, file2}, files.All())
}
