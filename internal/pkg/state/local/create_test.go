package local

import (
	jsonlib "encoding/json"
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
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
	manager := newTestLocalManager(t, []*storageapi.Component{component})
	key := model.ConfigKey{
		BranchId:    123,
		ComponentId: `keboola.foo`,
		Id:          "456",
	}
	object, err := manager.createObject(key, "New Config")
	assert.NoError(t, err)

	// Assert
	config := object.(*model.Config)
	assert.Equal(t, key, config.Key())
	assert.Equal(t, "New Config", config.Name)
	expectedContent := `{"configValue":123,"configObject":{"Key":"foo","Value":"bar"}}`
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
	manager := newTestLocalManager(t, []*storageapi.Component{component})
	key := model.ConfigRowKey{
		BranchId:    123,
		ComponentId: `keboola.foo`,
		ConfigId:    "567",
		Id:          "789",
	}
	object, err := manager.createObject(key, "New Row")
	assert.NoError(t, err)

	// Assert
	row := object.(*model.ConfigRow)
	assert.Equal(t, key, row.Key())
	assert.Equal(t, "New Row", row.Name)
	expectedContent := `{"configValue":123,"configObject":{"Key":"foo","Value":"bar"}}`
	assert.Equal(t, expectedContent, json.MustEncodeString(row.Content, false))
}

func TestLocalCreateConfigContentFromSchema(t *testing.T) {
	t.Parallel()

	// Mocked component
	component := getTestComponent()

	// Schema (used)
	component.Schema = getTestSchema()

	// Create
	manager := newTestLocalManager(t, []*storageapi.Component{component})
	key := model.ConfigKey{
		BranchId:    123,
		ComponentId: `keboola.foo`,
		Id:          "456",
	}
	object, err := manager.createObject(key, "New Config")
	assert.NoError(t, err)

	// Assert
	config := object.(*model.Config)
	assert.Equal(t, key, config.Key())
	assert.Equal(t, "New Config", config.Name)
	expectedContent := `{"bar":{"type":"abc"},"baz":{"type":789}}`
	assert.Equal(t, expectedContent, json.MustEncodeString(config.Content, false))
}

func TestLocalCreateConfigRowContentFromSchema(t *testing.T) {
	t.Parallel()

	// Mocked component
	component := getTestComponent()

	// Schema (used)
	component.SchemaRow = getTestSchema()

	// Create
	manager := newTestLocalManager(t, []*storageapi.Component{component})
	key := model.ConfigRowKey{
		BranchId:    123,
		ComponentId: `keboola.foo`,
		ConfigId:    "567",
		Id:          "789",
	}
	object, err := manager.createObject(key, "New Row")
	assert.NoError(t, err)

	// Assert
	row := object.(*model.ConfigRow)
	assert.Equal(t, key, row.Key())
	assert.Equal(t, "New Row", row.Name)
	expectedContent := `{"bar":{"type":"abc"},"baz":{"type":789}}`
	assert.Equal(t, expectedContent, json.MustEncodeString(row.Content, false))
}

func TestLocalCreateConfigEmptyContent(t *testing.T) {
	t.Parallel()
	// Mocked component
	component := getTestComponent()

	// Create
	manager := newTestLocalManager(t, []*storageapi.Component{component})
	key := model.ConfigKey{
		BranchId:    123,
		ComponentId: `keboola.foo`,
		Id:          "456",
	}
	object, err := manager.createObject(key, "New Config")
	assert.NoError(t, err)

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
	manager := newTestLocalManager(t, []*storageapi.Component{component})
	key := model.ConfigRowKey{
		BranchId:    123,
		ComponentId: `keboola.foo`,
		ConfigId:    "567",
		Id:          "789",
	}
	object, err := manager.createObject(key, "New Row")
	assert.NoError(t, err)

	// Assert
	row := object.(*model.ConfigRow)
	assert.Equal(t, key, row.Key())
	assert.Equal(t, "New Row", row.Name)
	expectedContent := `{}`
	assert.Equal(t, expectedContent, json.MustEncodeString(row.Content, false))
}

func getTestComponent() *storageapi.Component {
	return &storageapi.Component{
		ComponentKey:   storageapi.ComponentKey{ID: `keboola.foo`},
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
