package data

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestGenerateConfigJSON_StandardComponent(t *testing.T) {
	t.Parallel()

	// Create temp directory
	tmpDir := t.TempDir()

	// Create a config with standard parameters
	content := orderedmap.New()
	params := orderedmap.New()
	params.Set("key1", "value1")
	params.Set("key2", "value2")
	content.Set("parameters", params)

	config := &model.Config{
		ConfigKey: model.ConfigKey{
			ComponentID: keboola.ComponentID("keboola.python-transformation-v2"),
		},
		Content: content,
	}

	// Generate config.json
	err := generateConfigJSON(config, tmpDir)
	require.NoError(t, err)

	// Read and verify
	configPath := filepath.Join(tmpDir, "config.json")
	data, err := os.ReadFile(configPath)
	require.NoError(t, err)

	var result map[string]any
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	// Verify standard behavior: parameters is preserved as-is
	params2, ok := result["parameters"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "value1", params2["key1"])
	assert.Equal(t, "value2", params2["key2"])

	// No storage in output for standard components
	_, hasStorage := result["storage"]
	assert.False(t, hasStorage)
}

func TestGenerateConfigJSON_CustomPython(t *testing.T) {
	t.Parallel()

	// Create temp directory
	tmpDir := t.TempDir()

	// Create a config with kds-team.app-custom-python structure:
	// parameters.user_properties contains the actual user parameters
	// storage contains input/output mappings
	content := orderedmap.New()

	// Parameters with user_properties nested inside
	params := orderedmap.New()
	userProps := orderedmap.New()
	userProps.Set("my_param", "my_value")
	userProps.Set("another_param", 123)
	params.Set("user_properties", userProps)
	params.Set("some_internal_param", "internal") // This should NOT appear in output
	content.Set("parameters", params)

	// Storage section
	storage := orderedmap.New()
	inputSection := orderedmap.New()
	inputSection.Set("tables", []any{
		map[string]any{
			"source":      "in.c-bucket.table",
			"destination": "input.csv",
		},
	})
	storage.Set("input", inputSection)
	content.Set("storage", storage)

	config := &model.Config{
		ConfigKey: model.ConfigKey{
			ComponentID: keboola.ComponentID("kds-team.app-custom-python"),
		},
		Content: content,
	}

	// Generate config.json
	err := generateConfigJSON(config, tmpDir)
	require.NoError(t, err)

	// Read and verify
	configPath := filepath.Join(tmpDir, "config.json")
	data, err := os.ReadFile(configPath)
	require.NoError(t, err)

	var result map[string]any
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	// Verify: parameters should be the content of user_properties, not full parameters
	params2, ok := result["parameters"].(map[string]any)
	require.True(t, ok, "parameters should exist")
	assert.Equal(t, "my_value", params2["my_param"])
	assert.Equal(t, float64(123), params2["another_param"]) // JSON numbers are float64
	// Internal params should NOT be present
	_, hasInternal := params2["some_internal_param"]
	assert.False(t, hasInternal, "internal params should not be in output")

	// Verify: storage should be present
	storage2, ok := result["storage"].(map[string]any)
	require.True(t, ok, "storage should exist")
	input2, ok := storage2["input"].(map[string]any)
	require.True(t, ok, "storage.input should exist")
	tables, ok := input2["tables"].([]any)
	require.True(t, ok, "storage.input.tables should exist")
	assert.Len(t, tables, 1)
}

func TestGenerateConfigJSON_CustomPython_NoUserProperties(t *testing.T) {
	t.Parallel()

	// Create temp directory
	tmpDir := t.TempDir()

	// Create a config with kds-team.app-custom-python but no user_properties
	content := orderedmap.New()

	params := orderedmap.New()
	params.Set("some_param", "value")
	// No user_properties
	content.Set("parameters", params)

	config := &model.Config{
		ConfigKey: model.ConfigKey{
			ComponentID: keboola.ComponentID("kds-team.app-custom-python"),
		},
		Content: content,
	}

	// Generate config.json
	err := generateConfigJSON(config, tmpDir)
	require.NoError(t, err)

	// Read and verify
	configPath := filepath.Join(tmpDir, "config.json")
	data, err := os.ReadFile(configPath)
	require.NoError(t, err)

	var result map[string]any
	err = json.Unmarshal(data, &result)
	require.NoError(t, err)

	// Verify: parameters should not exist (no user_properties to extract)
	_, hasParams := result["parameters"]
	assert.False(t, hasParams, "parameters should not exist when user_properties is missing")
}

func TestGenerateCustomPythonConfig(t *testing.T) {
	t.Parallel()

	// Create content with user_properties and storage
	content := orderedmap.New()

	params := orderedmap.New()
	userProps := orderedmap.New()
	userProps.Set("key", "value")
	params.Set("user_properties", userProps)
	content.Set("parameters", params)

	storage := orderedmap.New()
	storage.Set("input", map[string]any{"tables": []any{}})
	content.Set("storage", storage)

	result := generateCustomPythonConfig(content)

	// Verify user_properties became parameters
	paramsResult, ok := result["parameters"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "value", paramsResult["key"])

	// Verify storage is preserved
	storageResult, ok := result["storage"].(map[string]any)
	require.True(t, ok)
	assert.NotNil(t, storageResult["input"])
}
