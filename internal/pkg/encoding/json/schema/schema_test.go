package schema_test

import (
	"context"
	"strings"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, ValidateContent(schema, content))
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
	require.Error(t, err)
	expectedErr := `
- missing properties: "firstName"
- "address": missing properties: "street"
- "address.number": expected integer, but got string
- "age": must be >= 0 but found -1
`
	assert.Equal(t, strings.TrimSpace(expectedErr), err.Error())
}

func TestValidateObjects_InvalidSchema_InvalidJSON(t *testing.T) {
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
	require.Error(t, err)
	expected := `
invalid JSON schema:
- invalid character '.' looking for beginning of object key string, offset: 2
`
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}

func TestValidateObjects_Ignore_TestConnectionButton(t *testing.T) {
	t.Parallel()
	invalidSchema := []byte(`
{
  "type": "object",
  "required": ["id"],
  "properties": {
    "id": {"type": "object"},
    "test_connection": {
      "type": "button",
      "format": "sync-action",
      "propertyOrder": 30,
      "options": {
        "async": {
          "label": "TEST CONNECTION",
          "action": "validate_connection"
        }
      }
    }
  }
}
`)

	err := ValidateContent(invalidSchema, orderedmap.FromPairs([]orderedmap.Pair{{Key: "parameters", Value: orderedmap.FromPairs([]orderedmap.Pair{{Key: "key", Value: "value"}})}}))
	if assert.Error(t, err) {
		assert.Equal(t, `missing properties: "id"`, err.Error())
	}
}

func TestValidateObjects_Ignore_ValidationButton(t *testing.T) {
	t.Parallel()
	invalidSchema := []byte(`
{
  "type": "object",
  "required": ["id"],
  "properties": {
    "id": {"type": "object"},
    "validation_button": {
      "type": "button",
      "format": "sync-action",
      "propertyOrder": 10,
      "options": {
        "async": {
          "label": "Validate",
          "action": "validate_report"
        }
      }
    }
  }
}
`)

	err := ValidateContent(invalidSchema, orderedmap.FromPairs([]orderedmap.Pair{{Key: "parameters", Value: orderedmap.FromPairs([]orderedmap.Pair{{Key: "key", Value: "value"}})}}))
	if assert.Error(t, err) {
		assert.Equal(t, `missing properties: "id"`, err.Error())
	}
}

func TestValidateObjects_Ignore_DynamicSingleSelect(t *testing.T) {
	t.Parallel()
	invalidSchema := []byte(`
{
  "type": "object",
  "required": ["id"],
  "properties": {
    "id": {"type": "object"},
    "test_columns_single": {
      "propertyOrder": 40,
      "type": "string",
      "description": "Element loaded by an arbitrary sync action. (single)",
      "enum": [],
      "format": "select",
      "options": {
        "async": {
          "label": "Re-load test columns",
          "action": "testColumns"
        }
      }
    }
  }
}
`)

	err := ValidateContent(invalidSchema, orderedmap.FromPairs([]orderedmap.Pair{{Key: "parameters", Value: orderedmap.FromPairs([]orderedmap.Pair{{Key: "key", Value: "value"}})}}))
	if assert.Error(t, err) {
		assert.Equal(t, `missing properties: "id"`, err.Error())
	}
}

func TestValidateObjects_Ignore_DynamicMultiSelect(t *testing.T) {
	t.Parallel()
	invalidSchema := []byte(`
{
  "type": "object",
  "required": ["id"],
  "properties": {
    "id": {"type": "object"},
    "test_columns": {
      "type": "array",
      "propertyOrder": 10,
      "description": "Element loaded by an arbitrary sync action.",
      "items": {
        "enum": [],
        "type": "string"
      },
      "format": "select",
      "options": {
        "async": {
          "label": "Re-load test columns",
          "action": "testColumns"
        }
      },
      "uniqueItems": true
    }
  }
}
`)

	err := ValidateContent(invalidSchema, orderedmap.FromPairs([]orderedmap.Pair{{Key: "parameters", Value: orderedmap.FromPairs([]orderedmap.Pair{{Key: "key", Value: "value"}})}}))
	if assert.Error(t, err) {
		assert.Equal(t, `missing properties: "id"`, err.Error())
	}
}

func TestValidateObjects_InvalidSchema_InvalidType(t *testing.T) {
	t.Parallel()
	invalidSchema := []byte(`{"properties":false}`)
	err := ValidateContent(invalidSchema, orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key:   "parameters",
			Value: orderedmap.FromPairs([]orderedmap.Pair{{Key: "key", Value: "value"}}),
		},
	}))
	require.Error(t, err)
	expected := `
invalid JSON schema:
- allOf failed:
  - doesn't validate with "https://json-schema.org/draft/2020-12/meta/applicator#":
    - "properties" is invalid: expected object, but got boolean
`
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}

func TestValidateObjects_BooleanRequired(t *testing.T) {
	t.Parallel()
	invalidSchema := []byte(`{"properties": {"key1": {"required": true}}}`)

	// Required field in a JSON schema should be an array of required nested fields.
	// But, for historical reasons, in Keboola components, "required: true" is also used.
	// In the UI, this causes the drop-down list to not have an empty value.
	// For this reason,the error should be ignored.
	require.NoError(t, ValidateContent(invalidSchema, orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "parameters",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "key1", Value: "value1"},
				{Key: "key2", Value: "value2"},
			}),
		},
	})))
}

func TestValidateObjects_SkipEmpty(t *testing.T) {
	t.Parallel()
	schema := getTestSchema()
	content := orderedmap.New()
	require.NoError(t, ValidateContent(schema, content))
}

func TestValidateObjects_InvalidSchema_Warning1(t *testing.T) {
	t.Parallel()
	invalidSchema := []byte(`{"properties": {"key1": {"properties": true}}}`)
	expectedLogs := `
WARN  config JSON schema of the component "foo.bar" is invalid, please contact support:
- allOf failed:
  - doesn't validate with "https://json-schema.org/draft/2020-12/meta/applicator#":
    - doesn't validate with "https://json-schema.org/draft/2020-12/schema#":
      - allOf failed:
        - doesn't validate with "https://json-schema.org/draft/2020-12/meta/applicator#":
          - "properties.key1.properties" is invalid: expected object, but got boolean
WARN  config row JSON schema of the component "foo.bar" is invalid, please contact support:
- allOf failed:
  - doesn't validate with "https://json-schema.org/draft/2020-12/meta/applicator#":
    - doesn't validate with "https://json-schema.org/draft/2020-12/schema#":
      - allOf failed:
        - doesn't validate with "https://json-schema.org/draft/2020-12/meta/applicator#":
          - "properties.key1.properties" is invalid: expected object, but got boolean
`
	testInvalidComponentSchema(t, invalidSchema, expectedLogs)
}

func TestValidateObjects_InvalidSchema_Warning2(t *testing.T) {
	t.Parallel()
	invalidSchema := []byte(`
{
  "type": "object",
  "$schema": "http://json-schema.org/draft-04/schema#",
  "properties": {
    "foo": {
      "type": "bar"
    }
  }
}
`)
	expectedLogs := `
WARN  config JSON schema of the component "foo.bar" is invalid, please contact support:
- anyOf failed:
  - doesn't validate with "/definitions/simpleTypes":
    - "properties.foo.type" is invalid: value must be one of "array", "boolean", "integer", "null", "number", "object", "string"
  - "properties.foo.type" is invalid: expected array, but got string
WARN  config row JSON schema of the component "foo.bar" is invalid, please contact support:
- anyOf failed:
  - doesn't validate with "/definitions/simpleTypes":
    - "properties.foo.type" is invalid: value must be one of "array", "boolean", "integer", "null", "number", "object", "string"
  - "properties.foo.type" is invalid: expected array, but got string
`
	testInvalidComponentSchema(t, invalidSchema, expectedLogs)
}

func testInvalidComponentSchema(t *testing.T, invalidSchema []byte, expectedLogs string) {
	t.Helper()

	// Create component, config and row definitions
	logger := log.NewDebugLogger()
	componentID := keboola.ComponentID("foo.bar")
	components := model.NewComponentsMap(keboola.Components{
		{
			ComponentKey: keboola.ComponentKey{ID: componentID},
			Type:         "other",
			Name:         "Foo Bar",
			Data:         keboola.ComponentData{},
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
	registry := state.NewRegistry(knownpaths.NewNop(context.Background()), naming.NewRegistry(), components, model.SortByID)
	require.NoError(t, registry.Set(&model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: model.ConfigKey{ComponentID: componentID}},
		Local:          &model.Config{Content: someContent},
	}))
	require.NoError(t, registry.Set(&model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{ConfigRowKey: model.ConfigRowKey{ComponentID: componentID}},
		Local:             &model.ConfigRow{Content: someContent},
	}))

	// Validate, no error
	content := orderedmap.New()
	content.Set(`parameters`, orderedmap.New())
	require.NoError(t, ValidateObjects(context.Background(), logger, registry))
	assert.Equal(t, strings.TrimLeft(expectedLogs, "\n"), logger.AllMessagesTxt())
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
