package model

import (
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
)

func TestFilesToSave(t *testing.T) {
	t.Parallel()
	files := NewFilesToSave()

	// Add file
	fileRaw1 := filesystem.NewRawFile(`path1`, `content1`)
	file1 := files.Add(fileRaw1)
	assert.Same(t, fileRaw1, file1)
	result, err := fileRaw1.ToRawFile()
	require.NoError(t, err)
	assert.Same(t, fileRaw1, result)
	assert.Equal(t, fileRaw1.Path(), file1.Path())

	// Add tag 1
	tag1 := `my-tag-1`
	assert.False(t, file1.HasTag(tag1))
	assert.Empty(t, files.GetByTag(tag1))
	assert.Nil(t, files.GetOneByTag(tag1))
	file1.AddTag(tag1)
	assert.True(t, file1.HasTag(tag1))
	assert.Equal(t, []filesystem.File{file1}, files.GetByTag(tag1))
	assert.Equal(t, file1, files.GetOneByTag(tag1))

	// Add tag 2
	tag2 := `my-tag-2`
	assert.False(t, file1.HasTag(tag2))
	assert.Empty(t, files.GetByTag(tag2))
	assert.Nil(t, files.GetOneByTag(tag2))
	file1.AddTag(tag2)
	assert.True(t, file1.HasTag(tag2))
	assert.Equal(t, []filesystem.File{file1}, files.GetByTag(tag2))
	assert.Equal(t, file1, files.GetOneByTag(tag2))

	// Two files with same tag
	fileRaw2 := filesystem.NewRawFile(`path2`, `content2`)
	file2 := files.Add(fileRaw2)
	assert.False(t, file2.HasTag(tag2))
	file2.AddTag(tag2)
	assert.True(t, file2.HasTag(tag2))
	assert.Equal(t, []filesystem.File{file1, file2}, files.GetByTag(tag2))
	assert.PanicsWithError(t, `found multiple files with tag "my-tag-2": "path1", "path2"`, func() {
		files.GetOneByTag(tag2)
	})

	// Delete tag which is not set
	file1.RemoveTag(`my-tag-3`)

	// Delete tag which is not set
	file1.RemoveTag(tag2)
	assert.False(t, file1.HasTag(tag2))
	assert.True(t, file2.HasTag(tag2))
	assert.Equal(t, []filesystem.File{file2}, files.GetByTag(tag2))
	assert.Equal(t, file2, files.GetOneByTag(tag2))

	// All
	assert.Equal(t, []filesystem.File{file1, file2}, files.All())
}

type myStruct struct {
	Field1   string                 `json:"field1" mytag:"field"`
	Field2   string                 `json:"field2" mytag:"field"`
	FooField string                 `json:"foo"`
	Map      *orderedmap.OrderedMap `mytag:"map"`
	Content  string                 `mytag:"content"`
}

func TestFilesLoader(t *testing.T) {
	t.Parallel()
	fs := aferofs.NewMemoryFs()
	files := NewFilesLoader(fs.FileLoader())

	// No files
	assert.Empty(t, files.Loaded())

	// Create files
	jsonContent := "{\"field1\": \"foo\", \"field2\": \"bar\"}"
	jsonMap := orderedmap.FromPairs([]orderedmap.Pair{{Key: "field1", Value: "foo"}, {Key: "field2", Value: "bar"}})
	require.NoError(t, fs.WriteFile(t.Context(), filesystem.NewRawFile("foo1.json", jsonContent)))
	require.NoError(t, fs.WriteFile(t.Context(), filesystem.NewRawFile("foo2.json", jsonContent)))
	require.NoError(t, fs.WriteFile(t.Context(), filesystem.NewRawFile("foo3.json", jsonContent)))
	require.NoError(t, fs.WriteFile(t.Context(), filesystem.NewRawFile("foo4.json", jsonContent)))
	require.NoError(t, fs.WriteFile(t.Context(), filesystem.NewRawFile("foo5.json", jsonContent)))

	// ReadFile
	rawFile1, err := files.
		Load(`foo1.json`).
		SetDescription(`my description`).
		AddTag(`tag1`).
		AddTag(`tag2`).
		ReadFile(t.Context())
	require.NoError(t, err)
	assert.Equal(t, `foo1.json`, rawFile1.Path())
	assert.Equal(t, `my description`, rawFile1.Description())
	assert.JSONEq(t, jsonContent, rawFile1.Content)

	// ReadJSONFile
	jsonFile1, err := files.
		Load(`foo2.json`).
		SetDescription(`my description`).
		AddTag(`tag3`).
		AddTag(`tag4`).
		ReadJSONFile(t.Context())
	require.NoError(t, err)
	assert.Equal(t, `foo2.json`, jsonFile1.Path())
	assert.Equal(t, `my description`, jsonFile1.Description())
	assert.Equal(t, jsonMap, jsonFile1.Content)

	// ReadJSONFieldsTo
	target1 := &myStruct{}
	jsonFile2, tagFound, err := files.
		Load(`foo3.json`).
		SetDescription(`my description`).
		AddTag(`tag5`).
		AddTag(`tag6`).
		ReadJSONFieldsTo(t.Context(), target1, `mytag:field`)
	assert.True(t, tagFound)
	require.NoError(t, err)
	assert.Equal(t, `foo3.json`, jsonFile2.Path())
	assert.Equal(t, `my description`, jsonFile2.Description())
	assert.Equal(t, `foo`, target1.Field1)
	assert.Equal(t, `bar`, target1.Field2)

	// ReadFileContentTo
	target2 := &myStruct{}
	rawFile2, tagFound, err := files.
		Load(`foo4.json`).
		SetDescription(`my description`).
		AddTag(`tag7`).
		AddTag(`tag8`).
		ReadFileContentTo(t.Context(), target2, `mytag:content`)
	assert.True(t, tagFound)
	require.NoError(t, err)
	assert.Equal(t, `foo4.json`, rawFile2.Path())
	assert.Equal(t, `my description`, rawFile2.Description())
	assert.JSONEq(t, jsonContent, target2.Content)

	// ReadJSONMapTo
	target3 := &myStruct{}
	jsonFile3, tagFound, err := files.
		Load(`foo5.json`).
		SetDescription(`my description`).
		AddTag(`tag9`).
		AddTag(`tag10`).
		ReadJSONMapTo(t.Context(), target3, `mytag:map`)
	assert.True(t, tagFound)
	require.NoError(t, err)
	assert.Equal(t, `foo5.json`, jsonFile3.Path())
	assert.Equal(t, `my description`, jsonFile3.Description())
	assert.Equal(t, jsonMap, target3.Map)

	// Check loaded files
	assert.Len(t, files.Loaded(), 5)
	assert.Equal(t, rawFile1, files.GetOneByTag("tag1"))
	assert.Equal(t, rawFile1, files.GetOneByTag("tag2"))
	assert.Equal(t, jsonFile1, files.GetOneByTag("tag3"))
	assert.Equal(t, jsonFile1, files.GetOneByTag("tag4"))
	assert.Equal(t, jsonFile2, files.GetOneByTag("tag5"))
	assert.Equal(t, jsonFile2, files.GetOneByTag("tag6"))
	assert.Equal(t, rawFile2, files.GetOneByTag("tag7"))
	assert.Equal(t, rawFile2, files.GetOneByTag("tag8"))
	assert.Equal(t, jsonFile3, files.GetOneByTag("tag9"))
	assert.Equal(t, jsonFile3, files.GetOneByTag("tag10"))
	assert.Nil(t, files.GetOneByTag("missing"))
}
