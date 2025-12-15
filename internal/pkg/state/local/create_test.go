package local

import (
	jsonlib "encoding/json"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestLocalCreateConfigDefaultContent(t *testing.T) {
	t.Parallel()

	// Mocked component
	component := getTestComponent()
	// Empty config (used)
	component.EmptyConfig = orderedmap.FromPairs([]orderedmap.Pair{
		{Key: `configValue`, Value: 123},
		{Key: `configObject`, Value: orderedmap.Pair{
			Key:   `foo`,
			Value: `bar`,
		}},
	})

	// Schema (not used)
	component.Schema = getTestSchema()

	// Create
	manager := newTestLocalManager(t, []*keboola.Component{component})
	key := model.ConfigKey{
		BranchID:    123,
		ComponentID: `keboola.foo`,
		ID:          "456",
	}
	object, err := manager.createObject(key, "New Config")
	require.NoError(t, err)

	// Assert
	config := object.(*model.Config)
	assert.Equal(t, key, config.Key())
	assert.Equal(t, "New Config", config.Name)
	expectedContent := `{"parameters":{"configValue":123,"configObject":{"Key":"foo","Value":"bar"}}}`
	assert.Equal(t, expectedContent, json.MustEncodeString(config.Content, false))
}

func TestLocalCreateConfigRowDefaultContent(t *testing.T) {
	t.Parallel()

	// Mocked component
	component := getTestComponent()

	// Empty config (used)
	component.EmptyConfigRow = orderedmap.FromPairs([]orderedmap.Pair{
		{Key: `configValue`, Value: 123},
		{Key: `configObject`, Value: orderedmap.Pair{
			Key:   `foo`,
			Value: `bar`,
		}},
	})

	// Schema (not used)
	component.Schema = getTestSchema()

	// Create
	manager := newTestLocalManager(t, []*keboola.Component{component})
	key := model.ConfigRowKey{
		BranchID:    123,
		ComponentID: `keboola.foo`,
		ConfigID:    "567",
		ID:          "789",
	}
	object, err := manager.createObject(key, "New Row")
	require.NoError(t, err)

	// Assert
	row := object.(*model.ConfigRow)
	assert.Equal(t, key, row.Key())
	assert.Equal(t, "New Row", row.Name)
	expectedContent := `{"parameters":{"configValue":123,"configObject":{"Key":"foo","Value":"bar"}}}`
	assert.Equal(t, expectedContent, json.MustEncodeString(row.Content, false))
}

func TestLocalCreateConfigContentFromSchema(t *testing.T) {
	t.Parallel()

	// Mocked component
	component := getTestComponent()

	// Schema (used)
	component.Schema = getTestSchema()

	// Create
	manager := newTestLocalManager(t, []*keboola.Component{component})
	key := model.ConfigKey{
		BranchID:    123,
		ComponentID: `keboola.foo`,
		ID:          "456",
	}
	object, err := manager.createObject(key, "New Config")
	require.NoError(t, err)

	// Assert
	config := object.(*model.Config)
	assert.Equal(t, key, config.Key())
	assert.Equal(t, "New Config", config.Name)
	expectedContent := `{"parameters":{"bar":{"type":"abc"},"baz":{"type":789}}}`
	assert.Equal(t, expectedContent, json.MustEncodeString(config.Content, false))
}

func TestLocalCreateConfigRowContentFromSchema(t *testing.T) {
	t.Parallel()

	// Mocked component
	component := getTestComponent()

	// Schema (used)
	component.SchemaRow = getTestSchema()

	// Create
	manager := newTestLocalManager(t, []*keboola.Component{component})
	key := model.ConfigRowKey{
		BranchID:    123,
		ComponentID: `keboola.foo`,
		ConfigID:    "567",
		ID:          "789",
	}
	object, err := manager.createObject(key, "New Row")
	require.NoError(t, err)

	// Assert
	row := object.(*model.ConfigRow)
	assert.Equal(t, key, row.Key())
	assert.Equal(t, "New Row", row.Name)
	expectedContent := `{"parameters":{"bar":{"type":"abc"},"baz":{"type":789}}}`
	assert.Equal(t, expectedContent, json.MustEncodeString(row.Content, false))
}

func TestLocalCreateConfigEmptyContent(t *testing.T) {
	t.Parallel()
	// Mocked component
	component := getTestComponent()

	// Create
	manager := newTestLocalManager(t, []*keboola.Component{component})
	key := model.ConfigKey{
		BranchID:    123,
		ComponentID: `keboola.foo`,
		ID:          "456",
	}
	object, err := manager.createObject(key, "New Config")
	require.NoError(t, err)

	// Assert
	config := object.(*model.Config)
	assert.Equal(t, key, config.Key())
	assert.Equal(t, "New Config", config.Name)
	expectedContent := `{}`
	assert.Equal(t, expectedContent, json.MustEncodeString(config.Content, false))
}

func TestLocalCreateConfigRowEmptyContent(t *testing.T) {
	t.Parallel()

	// Mocked component
	component := getTestComponent()

	// Create
	manager := newTestLocalManager(t, []*keboola.Component{component})
	key := model.ConfigRowKey{
		BranchID:    123,
		ComponentID: `keboola.foo`,
		ConfigID:    "567",
		ID:          "789",
	}
	object, err := manager.createObject(key, "New Row")
	require.NoError(t, err)

	// Assert
	row := object.(*model.ConfigRow)
	assert.Equal(t, key, row.Key())
	assert.Equal(t, "New Row", row.Name)
	expectedContent := `{}`
	assert.Equal(t, expectedContent, json.MustEncodeString(row.Content, false))
}

func TestLocalCreateTransformationWithBlocks(t *testing.T) {
	t.Parallel()

	// Mocked component - Python transformation v2
	component := &keboola.Component{
		ComponentKey:   keboola.ComponentKey{ID: `keboola.python-transformation-v2`},
		Type:           `transformation`,
		Flags:          []string{`genericCodeBlocksUI`},
		EmptyConfig:    orderedmap.New(),
		EmptyConfigRow: orderedmap.New(),
	}

	// Schema with blocks but WITHOUT minItems (simulating real API schema)
	component.Schema = jsonlib.RawMessage(`{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "object",
		"properties": {
			"blocks": {
				"type": "array",
				"items": {
					"type": "object",
					"properties": {
						"name": {
							"type": "string"
						},
						"codes": {
							"type": "array",
							"items": {
								"type": "object",
								"properties": {
									"name": {
										"type": "string"
									},
									"script": {
										"type": "array",
										"items": {
											"type": "string"
										}
									}
								}
							}
						}
					}
				}
			},
			"packages": {
				"type": "array",
				"items": {
					"type": "string"
				}
			}
		}
	}`)

	// Create
	manager := newTestLocalManager(t, []*keboola.Component{component})
	key := model.ConfigKey{
		BranchID:    123,
		ComponentID: `keboola.python-transformation-v2`,
		ID:          "456",
	}
	object, err := manager.createObject(key, "New Transformation")
	require.NoError(t, err)

	// Assert
	config := object.(*model.Config)
	assert.Equal(t, key, config.Key())
	assert.Equal(t, "New Transformation", config.Name)

	// Verify config content has at least one block and one code (ensured by ensureMinimalBlocks)
	expectedContent := `{"parameters":{"blocks":[{"name":"Block 1","codes":[{"name":"Code","script":[""]}]}],"packages":[]}}`
	assert.JSONEq(t, expectedContent, json.MustEncodeString(config.Content, false))

	// Verify Transformation field is initialized (blocks will be populated by MapBeforeLocalSave)
	require.NotNil(t, config.Transformation)
	// Blocks are empty here - they will be parsed from config.Content by MapBeforeLocalSave when saving
	assert.Equal(t, 0, len(config.Transformation.Blocks))
}

func TestLocalCreateTransformationWithBlocksAndEmptySchema(t *testing.T) {
	t.Parallel()

	// Mocked component - Python transformation v2 with empty schema
	component := &keboola.Component{
		ComponentKey:   keboola.ComponentKey{ID: `keboola.python-transformation-v2`},
		Type:           `transformation`,
		Flags:          []string{`genericCodeBlocksUI`},
		EmptyConfig:    orderedmap.New(),
		EmptyConfigRow: orderedmap.New(),
		Schema:         jsonlib.RawMessage(`{}`), // Empty schema
	}

	// Create
	manager := newTestLocalManager(t, []*keboola.Component{component})
	key := model.ConfigKey{
		BranchID:    123,
		ComponentID: `keboola.python-transformation-v2`,
		ID:          "456",
	}
	object, err := manager.createObject(key, "New Transformation")
	require.NoError(t, err)

	// Assert
	config := object.(*model.Config)
	assert.Equal(t, key, config.Key())
	assert.Equal(t, "New Transformation", config.Name)

	// Even with empty schema, should have at least one block and one code
	expectedContent := `{"parameters":{"blocks":[{"name":"Block 1","codes":[{"name":"Code","script":[""]}]}]}}`
	assert.JSONEq(t, expectedContent, json.MustEncodeString(config.Content, false))

	// Verify Transformation field is initialized
	require.NotNil(t, config.Transformation)
	assert.Equal(t, 0, len(config.Transformation.Blocks))
}

func getTestComponent() *keboola.Component {
	return &keboola.Component{
		ComponentKey:   keboola.ComponentKey{ID: `keboola.foo`},
		Type:           `other`,
		EmptyConfig:    orderedmap.New(),
		EmptyConfigRow: orderedmap.New(),
	}
}

func getTestSchema() jsonlib.RawMessage {
	return jsonlib.RawMessage(`
{
  "type": "object",
  "properties": {
    "bar": {
      "type": "object",
      "properties": {
        "type": {
          "type": "string",
          "enum": [
            "abc",
            "def"
          ]
        }
      }
    },
    "baz": {
      "type": "object",
      "properties": {
        "type": {
          "type": "number",
          "default": 789
        }
      }
    }
  }
}
`)
}
