package transformation

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestSaveTransformationEmpty(t *testing.T) {
	projectDir := t.TempDir()
	blocksDir := filepath.Join(projectDir, `branch`, `config`, `blocks`)
	assert.NoError(t, os.MkdirAll(blocksDir, 0755))

	// Prepare
	logger, _ := utils.NewDebugLogger()
	record, source := createTransTestStructs("keboola.snowflake-transformation")

	// Save
	configContent, err := SaveBlocks(projectDir, logger, model.DefaultNaming(), record, source)
	assert.NoError(t, err)
	configContentJson, err := json.Encode(configContent, false)
	assert.NoError(t, err)
	assert.Equal(t, `{}`, string(configContentJson))
}

func TestSaveTransformation(t *testing.T) {
	projectDir := t.TempDir()
	blocksDir := filepath.Join(projectDir, `branch`, `config`, `blocks`)
	assert.NoError(t, os.MkdirAll(blocksDir, 0755))

	// Prepare
	record, source := createTransTestStructs("keboola.snowflake-transformation")
	logger, logs := utils.NewDebugLogger()
	source.Content.Set(`foo`, `bar`)
	parameters := *utils.NewOrderedMap()
	parameters.Set(`blocks`, []map[string]interface{}{
		{
			"name": "block1",
			"codes": []map[string]interface{}{
				{
					"name": "code1",
					"scripts": []string{
						"SELECT 1",
					},
				},
				{
					"name": "code2",
					"scripts": []string{
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
					"name":    "code3",
					"scripts": []string{},
				},
			},
		},
	})
	source.Content.Set(`parameters`, parameters)

	// Save
	configContent, err := SaveBlocks(projectDir, logger, model.DefaultNaming(), record, source)
	assert.NoError(t, err)
	configContentJson, err := json.Encode(configContent, false)
	assert.NoError(t, err)

	// Blocks are not part of the config.json
	assert.Equal(t, `{"foo":"bar","parameters":{}}`, string(configContentJson))

	// Check logs
	expectedLogs := `
DEBUG  Saved "branch/config/.new_blocks/001-block-1/meta.json"
DEBUG  Saved "branch/config/.new_blocks/001-block-1/001-code-1/meta.json"
DEBUG  Saved "branch/config/.new_blocks/001-block-1/002-code-2/meta.json"
DEBUG  Saved "branch/config/.new_blocks/002-block-2/meta.json"
DEBUG  Saved "branch/config/.new_blocks/002-block-2/001-code-3/meta.json"
DEBUG  Moved "branch/config/.new_blocks" -> "branch/config/blocks"
`
	assert.Equal(t, strings.TrimLeft(expectedLogs, "\n"), logs.String())

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
