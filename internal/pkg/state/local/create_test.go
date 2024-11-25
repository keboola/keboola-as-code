package local

import (
	jsonlib "encoding/json"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
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
