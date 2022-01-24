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
	state := createStateWithMapper(t)

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
