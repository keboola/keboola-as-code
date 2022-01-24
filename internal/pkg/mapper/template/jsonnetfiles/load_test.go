package jsonnetfiles_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/fileloader"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestJsonNetMapper_LoadLocalFile(t *testing.T) {
	t.Parallel()
	state := createStateWithMapper(t, nil)

	// Write JsonNet file
	fs := state.Fs()
	jsonNetContent := `{ foo: "bar"}`
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(`my/dir/file.jsonnet`, jsonNetContent)))

	// Create file loader
	fileLoader := state.Mapper().NewFileLoader(fileloader.HandlerFromFs(fs))

	// Load file
	fileDef := filesystem.NewFileDef(`my/dir/file.json`)
	fileDef.AddTag(model.FileTypeJson)
	jsonFile, err := fileLoader.ReadJsonFile(fileDef)
	assert.NoError(t, err)

	// JsonNet file is loaded and converted to a Json file
	assert.Equal(t, `my/dir/file.jsonnet`, jsonFile.Path())
	assert.Equal(t, []string{model.FileTypeJsonNet}, jsonFile.AllTags())
	assert.Equal(t, orderedmap.FromPairs([]orderedmap.Pair{
		{Key: "foo", Value: "bar"},
	}), jsonFile.Content)
}

func TestJsonNetMapper_LoadLocalFile_Variables(t *testing.T) {
	t.Parallel()

	// Define variables
	variables := map[string]interface{}{
		"myNull":   nil,
		"myBool":   true,
		"myString": "myString",
		"myFloat":  12.34,
		"myInt":    789,
	}

	// Create state
	state := createStateWithMapper(t, variables)

	// Write JsonNet file
	fs := state.Fs()
	jsonNetContent := `{ 
  "null": std.extVar("myNull"),
  "bool": std.extVar("myBool"),
  "string": std.extVar("myString"),
  "float": std.extVar("myFloat"),
  "int": std.extVar("myInt"),
}`
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(`my/dir/file.jsonnet`, jsonNetContent)))

	// Create file loader
	fileLoader := state.Mapper().NewFileLoader(fileloader.HandlerFromFs(fs))

	// Load file
	fileDef := filesystem.NewFileDef(`my/dir/file.json`)
	fileDef.AddTag(model.FileTypeJson)
	jsonFile, err := fileLoader.ReadJsonFile(fileDef)
	assert.NoError(t, err)

	// JsonNet file is loaded and converted to a Json file
	assert.Equal(t, `my/dir/file.jsonnet`, jsonFile.Path())
	assert.Equal(t, []string{model.FileTypeJsonNet}, jsonFile.AllTags())
	assert.Equal(t, orderedmap.FromPairs([]orderedmap.Pair{
		{Key: "bool", Value: true},
		{Key: "float", Value: 12.34},
		{Key: "int", Value: float64(789)},
		{Key: "null", Value: nil},
		{Key: "string", Value: "myString"},
	}), jsonFile.Content)
}
