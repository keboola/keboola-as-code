package jsonnetfiles_test

import (
	"context"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestJsonnetMapper_LoadLocalFile(t *testing.T) {
	t.Parallel()

	// Variables
	jsonnetCtx := jsonnet.NewContext()
	jsonnetCtx.ExtVar("myKey", "bar")
	ctx := context.Background()

	// Create state
	state := createStateWithMapper(t, jsonnetCtx)

	// Write Jsonnet file with a variable
	fs := state.ObjectsRoot()
	jsonnetContent := `{ foo: std.extVar("myKey")}`
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(`my/dir/file.jsonnet`, jsonnetContent)))

	// Create file loader
	fileLoader := state.Mapper().NewFileLoader(fs)

	// Load file
	fileDef := filesystem.NewFileDef(`my/dir/file.json`)
	fileDef.AddTag(model.FileTypeJSON)
	jsonFile, err := fileLoader.ReadJSONFile(ctx, fileDef)
	require.NoError(t, err)

	// Jsonnet file is loaded and converted to a Json file
	assert.Equal(t, `my/dir/file.jsonnet`, jsonFile.Path())
	assert.Equal(t, []string{model.FileTypeJsonnet}, jsonFile.AllTags())
	assert.Equal(t, orderedmap.FromPairs([]orderedmap.Pair{
		{Key: "foo", Value: "bar"},
	}), jsonFile.Content)
}
