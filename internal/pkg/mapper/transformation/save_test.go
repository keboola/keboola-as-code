package transformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/transformation"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestSaveTransformationEmpty(t *testing.T) {
	t.Parallel()
	context, config, configRecord := createTestFixtures(t)
	recipe := createLocalSaveRecipe(config, configRecord)
	fs := context.Fs
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)
	assert.NoError(t, fs.Mkdir(blocksDir))

	// Save
	err := NewMapper(context).BeforeLocalSave(recipe)
	assert.NoError(t, err)
	configContent := json.MustEncodeString(recipe.Configuration.Content, false)
	assert.Equal(t, `{}`, configContent)
}

func TestSaveTransformation(t *testing.T) {
	t.Parallel()
	context, config, configRecord := createTestFixtures(t)
	recipe := createLocalSaveRecipe(config, configRecord)
	fs := context.Fs
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)
	assert.NoError(t, fs.Mkdir(blocksDir))

	// Prepare
	recipe.Configuration.Content.Set(`foo`, `bar`)
	parameters := *utils.NewOrderedMap()
	parameters.Set(`blocks`, []map[string]interface{}{
		{
			"name": "block1",
			"codes": []map[string]interface{}{
				{
					"name": "code1",
					"script": []string{
						"SELECT 1",
					},
				},
				{
					"name": "code2",
					"script": []string{
						"SELECT 2;",
						"SELECT 3;",
					},
				},
			},
		},
		{
			"name": "block2",
			"codes": []map[string]interface{}{
				{
					"name":   "code3",
					"script": []string{},
				},
			},
		},
	})
	recipe.Configuration.Content.Set(`parameters`, parameters)

	// Save
	assert.NoError(t, NewMapper(context).BeforeLocalSave(recipe))

	// Blocks are not part of the config.json
	configContent := json.MustEncodeString(recipe.Configuration.Content, false)
	assert.Equal(t, `{"foo":"bar","parameters":{}}`, configContent)

	// Check generated files
	assert.Equal(t, []*filesystem.File{
		filesystem.CreateFile(blocksDir+`/001-block-1/meta.json`, "{\n  \"name\": \"block1\"\n}\n").SetDescription(`block metadata`),
		filesystem.CreateFile(blocksDir+`/001-block-1/001-code-1/meta.json`, "{\n  \"name\": \"code1\"\n}\n").SetDescription(`code metadata`),
		filesystem.CreateFile(blocksDir+`/001-block-1/001-code-1/code.sql`, "SELECT 1\n").SetDescription(`code`),
		filesystem.CreateFile(blocksDir+`/001-block-1/002-code-2/meta.json`, "{\n  \"name\": \"code2\"\n}\n").SetDescription(`code metadata`),
		filesystem.CreateFile(blocksDir+`/001-block-1/002-code-2/code.sql`, "SELECT 2;\n\nSELECT 3;\n").SetDescription(`code`),
		filesystem.CreateFile(blocksDir+`/002-block-2/meta.json`, "{\n  \"name\": \"block2\"\n}\n").SetDescription(`block metadata`),
		filesystem.CreateFile(blocksDir+`/002-block-2/001-code-3/meta.json`, "{\n  \"name\": \"code3\"\n}\n").SetDescription(`code metadata`),
		filesystem.CreateFile(blocksDir+`/002-block-2/001-code-3/code.sql`, "\n").SetDescription(`code`),
	}, recipe.ExtraFiles)
}
