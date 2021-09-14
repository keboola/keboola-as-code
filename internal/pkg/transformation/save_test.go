package transformation

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"keboola-as-code/src/json"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
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

	// Check meta files content
	block1Meta, err := os.ReadFile(filepath.Join(blocksDir, `001-block-1/meta.json`))
	assert.NoError(t, err)
	assert.Equal(t, "{\n  \"name\": \"block1\"\n}\n", string(block1Meta))
	block2Meta, err := os.ReadFile(filepath.Join(blocksDir, `002-block-2/meta.json`))
	assert.NoError(t, err)
	assert.Equal(t, "{\n  \"name\": \"block2\"\n}\n", string(block2Meta))
	code1Meta, err := os.ReadFile(filepath.Join(blocksDir, `001-block-1/001-code-1/meta.json`))
	assert.NoError(t, err)
	assert.Equal(t, "{\n  \"name\": \"code1\"\n}\n", string(code1Meta))
	code2Meta, err := os.ReadFile(filepath.Join(blocksDir, `001-block-1/002-code-2/meta.json`))
	assert.NoError(t, err)
	assert.Equal(t, "{\n  \"name\": \"code2\"\n}\n", string(code2Meta))
	code3Meta, err := os.ReadFile(filepath.Join(blocksDir, `002-block-2/001-code-3/meta.json`))
	assert.NoError(t, err)
	assert.Equal(t, "{\n  \"name\": \"code3\"\n}\n", string(code3Meta))
}
