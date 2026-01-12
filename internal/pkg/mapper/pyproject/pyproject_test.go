package pyproject

import (
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestIsPythonTransformation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		componentID string
		expected    bool
	}{
		{"keboola.python-transformation-v2", true},
		{"keboola.csas-python-transformation-v2", true},
		{"keboola.python-mlflow", true},
		{"kds-team.app-custom-python", true},
		{"keboola.snowflake-transformation", false},
		{"keboola.r-transformation-v2", false},
		{"keboola.julia-transformation", false},
	}

	for _, tt := range tests {
		config := &model.Config{
			ConfigKey: model.ConfigKey{
				ComponentID: keboola.ComponentID(tt.componentID),
			},
		}
		assert.Equal(t, tt.expected, isPythonTransformation(config), "componentID: %s", tt.componentID)
	}
}

func TestGeneratePyProjectToml(t *testing.T) {
	t.Parallel()

	config := &model.Config{
		ConfigKey: model.ConfigKey{
			ComponentID: keboola.ComponentID("keboola.python-transformation-v2"),
			ID:          keboola.ConfigID("12345"),
		},
		Name: "My Python Transform",
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{
				Key: "parameters",
				Value: map[string]any{
					"packages": []any{"pandas>=2.0.0", "numpy", "scikit-learn==1.0.0"},
				},
			},
		}),
	}

	result := generatePyProjectToml(config)

	assert.Contains(t, result, `name = "my-python-transform"`)
	assert.Contains(t, result, `version = "1.0.0"`)
	assert.Contains(t, result, `requires-python = ">=3.11"`)
	assert.Contains(t, result, `"pandas>=2.0.0"`)
	assert.Contains(t, result, `"numpy"`)
	assert.Contains(t, result, `"scikit-learn==1.0.0"`)
	assert.Contains(t, result, `component_id = "keboola.python-transformation-v2"`)
	assert.Contains(t, result, `config_id = "12345"`)
}

func TestGetPackagesFromConfig(t *testing.T) {
	t.Parallel()

	// Test with []any packages
	config := &model.Config{
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{
				Key: "parameters",
				Value: map[string]any{
					"packages": []any{"pandas", "numpy"},
				},
			},
		}),
	}

	packages := getPackagesFromConfig(config)
	assert.Equal(t, []string{"pandas", "numpy"}, packages)

	// Test with no packages
	configNoPackages := &model.Config{
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{
				Key:   "parameters",
				Value: map[string]any{},
			},
		}),
	}

	packagesEmpty := getPackagesFromConfig(configNoPackages)
	assert.Empty(t, packagesEmpty)

	// Test with nil content
	configNil := &model.Config{}
	packagesNil := getPackagesFromConfig(configNil)
	assert.Empty(t, packagesNil)
}

func TestParsePyProjectPackages(t *testing.T) {
	t.Parallel()

	content := `[project]
name = "my-transform"
version = "1.0.0"

dependencies = [
    "pandas>=2.0.0",
    "numpy",
    "scikit-learn==1.0.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0.0",
]
`

	packages := parsePyProjectPackages(content)

	assert.Len(t, packages, 3)
	assert.Contains(t, packages, "pandas>=2.0.0")
	assert.Contains(t, packages, "numpy")
	assert.Contains(t, packages, "scikit-learn==1.0.0")
}

func TestParsePyProjectPackages_Empty(t *testing.T) {
	t.Parallel()

	content := `[project]
name = "my-transform"

dependencies = [
]
`

	packages := parsePyProjectPackages(content)
	assert.Empty(t, packages)
}

func TestUpdateConfigPackages_OrderedMap(t *testing.T) {
	t.Parallel()

	// Test with *orderedmap.OrderedMap parameters (this is how corefiles mapper stores them)
	params := orderedmap.New()
	params.Set("packages", []any{"old-package"})

	config := &model.Config{
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{Key: "parameters", Value: params},
		}),
	}

	// Update packages
	updateConfigPackages(config, []string{"pandas>=2.0.0", "numpy", "requests"})

	// Verify packages were updated
	parametersRaw, ok := config.Content.Get("parameters")
	assert.True(t, ok)

	paramsMap, ok := parametersRaw.(*orderedmap.OrderedMap)
	assert.True(t, ok)

	packagesRaw, ok := paramsMap.Get("packages")
	assert.True(t, ok)

	packages, ok := packagesRaw.([]any)
	assert.True(t, ok)
	assert.Len(t, packages, 3)
	assert.Equal(t, "pandas>=2.0.0", packages[0])
	assert.Equal(t, "numpy", packages[1])
	assert.Equal(t, "requests", packages[2])
}

func TestUpdateConfigPackages_MapStringAny(t *testing.T) {
	t.Parallel()

	// Test with map[string]any parameters
	config := &model.Config{
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{
				Key: "parameters",
				Value: map[string]any{
					"packages": []any{"old-package"},
				},
			},
		}),
	}

	// Update packages
	updateConfigPackages(config, []string{"pandas>=2.0.0", "numpy"})

	// Verify packages were updated
	parametersRaw, ok := config.Content.Get("parameters")
	assert.True(t, ok)

	paramsMap, ok := parametersRaw.(map[string]any)
	assert.True(t, ok)

	packages, ok := paramsMap["packages"].([]any)
	assert.True(t, ok)
	assert.Len(t, packages, 2)
	assert.Equal(t, "pandas>=2.0.0", packages[0])
	assert.Equal(t, "numpy", packages[1])
}

func TestUpdateConfigPackages_NoParameters(t *testing.T) {
	t.Parallel()

	// Test with no parameters - should create them
	config := &model.Config{
		Content: orderedmap.New(),
	}

	// Update packages
	updateConfigPackages(config, []string{"pandas>=2.0.0"})

	// Verify parameters and packages were created
	parametersRaw, ok := config.Content.Get("parameters")
	assert.True(t, ok)

	paramsMap, ok := parametersRaw.(*orderedmap.OrderedMap)
	assert.True(t, ok)

	packagesRaw, ok := paramsMap.Get("packages")
	assert.True(t, ok)

	packages, ok := packagesRaw.([]any)
	assert.True(t, ok)
	assert.Len(t, packages, 1)
	assert.Equal(t, "pandas>=2.0.0", packages[0])
}
