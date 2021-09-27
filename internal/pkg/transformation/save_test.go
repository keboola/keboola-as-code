package transformation

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestSaveTransformationEmpty(t *testing.T) {
	logger, fs, state, objectFiles := createTestFixtures(t)
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)
	assert.NoError(t, fs.Mkdir(blocksDir))

	// Save
	err := Save(logger, fs, model.DefaultNaming(), state, objectFiles)
	assert.NoError(t, err)
	configContent := json.MustEncodeString(objectFiles.Configuration.Content, false)
	assert.Equal(t, `{}`, configContent)
}

func TestSaveTransformation(t *testing.T) {
	logger, fs, state, objectFiles := createTestFixtures(t)
	blocksDir := filesystem.Join(`branch`, `config`, `blocks`)
	assert.NoError(t, fs.Mkdir(blocksDir))

	// Prepare
	objectFiles.Configuration.Content.Set(`foo`, `bar`)
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
	objectFiles.Configuration.Content.Set(`parameters`, parameters)

	// Save
	err := Save(logger, fs, model.DefaultNaming(), state, objectFiles)
	assert.NoError(t, err)

	// Blocks are not part of the config.json
	configContent := json.MustEncodeString(objectFiles.Configuration.Content, false)
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
	}, objectFiles.Extra)
}
