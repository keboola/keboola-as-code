package schema_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
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
- missing property "firstName"
- "address": missing property "street"
- "address.number": got string, want integer
- "age": minimum: got -1, want 0
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
invalid character '.' looking for beginning of object key string, offset: 2
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
		assert.Equal(t, `missing property "id"`, err.Error())
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
		assert.Equal(t, `missing property "id"`, err.Error())
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
		assert.Equal(t, `missing property "id"`, err.Error())
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
		assert.Equal(t, `missing property "id"`, err.Error())
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
- "file:///schema.json#" is not valid against metaschema: jsonschema validation failed with 'https://json-schema.org/draft/2020-12/schema#'
  - at '': 'allOf' failed
    - at '/properties': got boolean, want object
`
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}

func TestValidateObjects_BooleanRequired(t *testing.T) {
	t.Parallel()
	invalidSchema := []byte(`{"properties": {"key1": {"required": true}}}`)

	// Required field in a JSON schema should be an array of required nested fields.
	// But, for historical reasons, in Keboola components, "required: true" is also used.
	// In the UI, this causes the drop-down list to not have an empty value.
	// For this reason, the error should be ignored.
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

func TestValidateObjects_EmptyEnum(t *testing.T) {
	t.Parallel()
	invalidSchema := []byte(`{"properties": {"key1": {"enum": []}}}`)

	// Keboola is using enums with no options in the schema because the valid options are loaded dynamically.
	// Let's make sure that this does not cause the schema to be considered invalid.
	err := ValidateContent(invalidSchema, orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "parameters",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "key1", Value: "value1"},
			}),
		},
	}))
	require.Error(t, err)
	// An error is expected, it just shouldn't be a schema error.
	assert.Equal(t, "\"key1\": value must be one of ", err.Error())
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
- "file:///schema.json#" is not valid against metaschema: jsonschema validation failed with 'https://json-schema.org/draft/2020-12/schema#'
  - at '': 'allOf' failed
    - at '/properties/key1': 'allOf' failed
      - at '/properties/key1/properties': got boolean, want object
WARN  config row JSON schema of the component "foo.bar" is invalid, please contact support:
- "file:///schema.json#" is not valid against metaschema: jsonschema validation failed with 'https://json-schema.org/draft/2020-12/schema#'
  - at '': 'allOf' failed
    - at '/properties/key1': 'allOf' failed
      - at '/properties/key1/properties': got boolean, want object
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
- "file:///schema.json#" is not valid against metaschema: jsonschema validation failed with 'http://json-schema.org/draft-04/schema#'
  - at '/properties/foo/type': 'anyOf' failed
    - at '/properties/foo/type': value must be one of 'array', 'boolean', 'integer', 'null', 'number', 'object', 'string'
    - at '/properties/foo/type': got string, want array
WARN  config row JSON schema of the component "foo.bar" is invalid, please contact support:
- "file:///schema.json#" is not valid against metaschema: jsonschema validation failed with 'http://json-schema.org/draft-04/schema#'
  - at '/properties/foo/type': 'anyOf' failed
    - at '/properties/foo/type': value must be one of 'array', 'boolean', 'integer', 'null', 'number', 'object', 'string'
    - at '/properties/foo/type': got string, want array
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
	registry := state.NewRegistry(knownpaths.NewNop(t.Context()), naming.NewRegistry(), components, model.SortByID)
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
	require.NoError(t, ValidateObjects(t.Context(), logger, registry))
	assert.Equal(t, strings.TrimLeft(expectedLogs, "\n"), logger.AllMessagesTxt())
}

func TestNormalizeSchema_RequiredTrue(t *testing.T) {
	t.Parallel()

	schema := []byte(`
{
  "type": "object",
  "properties": {
    "address": {
      "type": "object",
      "properties": {
        "street": {
          "type": "string",
          "required": true
        },
        "number": {
          "type": "integer"
        }
      }
    }
  }
}
`)

	// Normalize the schema
	normalizedSchema, err := NormalizeSchema(schema)
	require.NoError(t, err)

	expectedSchema := []byte(`
{
  "type": "object",
  "properties": {
    "address": {
      "type": "object",
      "properties": {
        "street": {
          "type": "string"
        },
        "number": {
          "type": "integer"
        }
      }
    }
  }
}
`)
	var buf bytes.Buffer
	err = json.Compact(&buf, expectedSchema)
	require.NoError(t, err)
	expectedSchema = buf.Bytes()

	assert.Equal(t, strings.TrimSpace(string(expectedSchema)), strings.TrimSpace(string(normalizedSchema)))
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
