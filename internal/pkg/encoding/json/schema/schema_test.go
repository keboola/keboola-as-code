package schema_test

import (
	"strings"
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/encoding/json/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/knownpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func TestValidateObjects_Ok(t *testing.T) {
	t.Parallel()
	schema := getTestSchema()
	parameters := orderedmap.FromPairs([]orderedmap.Pair{
		{Key: "firstName", Value: "John"},
		{Key: "lastName", Value: "Brown"},
		{Key: "age", Value: 25},
	})
	content := orderedmap.New()
	content.Set(`parameters`, parameters)
	assert.NoError(t, ValidateContent(schema, content))
}

func TestValidateObjects_Error(t *testing.T) {
	t.Parallel()
	schema := getTestSchema()
	parameters := orderedmap.FromPairs([]orderedmap.Pair{
		{Key: "lastName", Value: "Brown"},
		{Key: "age", Value: -1},
		{
			Key: "address",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "number", Value: "abc"},
			}),
		},
	})
	content := orderedmap.New()
	content.Set(`parameters`, parameters)
	err := ValidateContent(schema, content)
	assert.Error(t, err)
	expectedErr := `
- missing properties: "firstName"
- "address": missing properties: "street"
- "address.number": expected integer, but got string
- "age": must be >= 0 but found -1
`
	assert.Equal(t, strings.TrimSpace(expectedErr), err.Error())
}

func TestValidateObjects_InvalidSchema_JSON(t *testing.T) {
	t.Parallel()
	invalidSchema := []byte(`{...`)
	err := ValidateContent(invalidSchema, orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "parameters",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "key1", Value: "value1"},
				{Key: "key2", Value: "value2"},
			}),
		},
	}))
	assert.Error(t, err)
	expected := `
invalid JSON schema:
- invalid character '.' looking for beginning of object key string
`
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}

func TestValidateObjects_InvalidSchema_FieldType(t *testing.T) {
	t.Parallel()
	invalidSchema := []byte(`{"properties":false}`)
	err := ValidateContent(invalidSchema, orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "parameters",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "key1", Value: "value1"},
				{Key: "key2", Value: "value2"},
			}),
		},
	}))
	assert.Error(t, err)
	expected := `
invalid JSON schema:
  - "properties" is invalid: expected object, but got boolean
`
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}

func TestValidateObjects_BooleanRequired(t *testing.T) {
	t.Parallel()
	invalidSchema := []byte(`{"properties": {"key1": {"required": true}}}`)
	err := ValidateContent(invalidSchema, orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "parameters",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "key1", Value: "value1"},
				{Key: "key2", Value: "value2"},
			}),
		},
	}))
	assert.Error(t, err)
	expected := `
invalid JSON schema:
  - "properties.key1.required" is invalid: expected array, but got boolean
`
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}

func TestValidateObjects_SkipEmpty(t *testing.T) {
	t.Parallel()
	schema := getTestSchema()
	content := orderedmap.New()
	assert.NoError(t, ValidateContent(schema, content))
}

func TestValidateObjects_InvalidSchema_Warning(t *testing.T) {
	t.Parallel()
	invalidSchema := []byte(`{"properties": {"key1": {"required": true}}}`)

	componentID := storageapi.ComponentID("foo.bar")
	components := model.NewComponentsMap(storageapi.Components{
		{
			ComponentKey: storageapi.ComponentKey{ID: componentID},
			Type:         "other",
			Name:         "Foo Bar",
			Data:         storageapi.ComponentData{},
			Schema:       invalidSchema,
			SchemaRow:    invalidSchema,
		},
	})
	someContent := orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "parameters",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "key1", Value: "value1"},
				{Key: "key2", Value: "value2"},
			}),
		},
	})

	logger := log.NewDebugLogger()
	registry := state.NewRegistry(knownpaths.NewNop(), naming.NewRegistry(), components, model.SortByID)
	assert.NoError(t, registry.Set(&model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: model.ConfigKey{ComponentID: componentID}},
		Local:          &model.Config{Content: someContent},
	}))
	assert.NoError(t, registry.Set(&model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{ConfigRowKey: model.ConfigRowKey{ComponentID: componentID}},
		Local:             &model.ConfigRow{Content: someContent},
	}))

	// Validate, no error
	content := orderedmap.New()
	content.Set(`parameters`, orderedmap.New())
	assert.NoError(t, ValidateObjects(logger, registry))

	// Check logs
	expected := `
WARN  config JSON schema of the component "foo.bar" is invalid, please contact support:
- "properties.key1.required" is invalid: expected array, but got boolean
WARN  config row JSON schema of the component "foo.bar" is invalid, please contact support:
- "properties.key1.required" is invalid: expected array, but got boolean
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), logger.AllMessages())
}

func getTestSchema() []byte {
	return []byte(`
{
  "required": [ "firstName", "lastName", "age"],
  "properties": {
    "firstName": {
      "type": "string"
    },
    "lastName": {
      "type": "string",
      "default": "Green"
    },
    "age": {
      "type": "integer",
      "minimum": 0
    },
    "address": {
      "type": "object",
      "required": ["street", "number"],
      "properties": {
        "street": {
          "type": "string",
          "default": "Street"
        },
        "number": {
          "type": "integer",
          "default": 123
        }
      }
    }
  }
}
`)
}
