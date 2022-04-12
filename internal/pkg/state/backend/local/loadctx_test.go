package local

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
)

func TestLoadContext(t *testing.T) {
	t.Parallel()
	fs := testfs.NewMemoryFs()
	ctx, err := NewLoadContext(context.Background(), fs.FileLoader(), nil, nil)
	assert.NoError(t, err)

	// No files
	assert.Empty(t, ctx.Loaded())

	// Create files
	jsonContent := "{\"field1\": \"foo\", \"field2\": \"bar\"}"
	jsonMap := orderedmap.FromPairs([]orderedmap.Pair{{Key: "field1", Value: "foo"}, {Key: "field2", Value: "bar"}})
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile("foo1.json", jsonContent)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile("foo2.json", jsonContent)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile("foo3.json", jsonContent)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile("foo4.json", jsonContent)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile("foo5.json", jsonContent)))

	// ReadFile
	rawFile1, err := ctx.
		Load(`foo1.json`).
		SetDescription(`my description`).
		AddTag(`tag1`).
		AddTag(`tag2`).
		ReadFile()
	assert.NoError(t, err)
	assert.Equal(t, `foo1.json`, rawFile1.Path())
	assert.Equal(t, `my description`, rawFile1.Description())
	assert.Equal(t, jsonContent, rawFile1.Content)

	// ReadJsonFile
	jsonFile1, err := ctx.
		Load(`foo2.json`).
		SetDescription(`my description`).
		AddTag(`tag3`).
		AddTag(`tag4`).
		ReadJsonFile()
	assert.NoError(t, err)
	assert.Equal(t, `foo2.json`, jsonFile1.Path())
	assert.Equal(t, `my description`, jsonFile1.Description())
	assert.Equal(t, jsonMap, jsonFile1.Content)

	// ReadJsonFieldsTo
	target1 := &myStruct{}
	jsonFile2, tagFound, err := ctx.
		Load(`foo3.json`).
		SetDescription(`my description`).
		AddTag(`tag5`).
		AddTag(`tag6`).
		ReadJsonFieldsTo(target1, `mytag:field`)
	assert.True(t, tagFound)
	assert.NoError(t, err)
	assert.Equal(t, `foo3.json`, jsonFile2.Path())
	assert.Equal(t, `my description`, jsonFile2.Description())
	assert.Equal(t, `foo`, target1.Field1)
	assert.Equal(t, `bar`, target1.Field2)

	// ReadFileContentTo
	target2 := &myStruct{}
	rawFile2, tagFound, err := ctx.
		Load(`foo4.json`).
		SetDescription(`my description`).
		AddTag(`tag7`).
		AddTag(`tag8`).
		ReadFileContentTo(target2, `mytag:content`)
	assert.True(t, tagFound)
	assert.NoError(t, err)
	assert.Equal(t, `foo4.json`, rawFile2.Path())
	assert.Equal(t, `my description`, rawFile2.Description())
	assert.Equal(t, jsonContent, target2.Content)

	// ReadJsonMapTo
	target3 := &myStruct{}
	jsonFile3, tagFound, err := ctx.
		Load(`foo5.json`).
		SetDescription(`my description`).
		AddTag(`tag9`).
		AddTag(`tag10`).
		ReadJsonMapTo(target3, `mytag:map`)
	assert.True(t, tagFound)
	assert.NoError(t, err)
	assert.Equal(t, `foo5.json`, jsonFile3.Path())
	assert.Equal(t, `my description`, jsonFile3.Description())
	assert.Equal(t, jsonMap, target3.Map)

	// Check loaded files
	assert.Len(t, ctx.Loaded(), 5)
	assert.Equal(t, rawFile1, ctx.GetOneByTag("tag1"))
	assert.Equal(t, rawFile1, ctx.GetOneByTag("tag2"))
	assert.Equal(t, jsonFile1, ctx.GetOneByTag("tag3"))
	assert.Equal(t, jsonFile1, ctx.GetOneByTag("tag4"))
	assert.Equal(t, jsonFile2, ctx.GetOneByTag("tag5"))
	assert.Equal(t, jsonFile2, ctx.GetOneByTag("tag6"))
	assert.Equal(t, rawFile2, ctx.GetOneByTag("tag7"))
	assert.Equal(t, rawFile2, ctx.GetOneByTag("tag8"))
	assert.Equal(t, jsonFile3, ctx.GetOneByTag("tag9"))
	assert.Equal(t, jsonFile3, ctx.GetOneByTag("tag10"))
	assert.Nil(t, ctx.GetOneByTag("missing"))
}
