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
	invalidSchema := []byte(`{"properties": {"key1": {"required": true}, "key2": {"required": false}}}`)

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
	schema := []byte(`
{
  "type": "object",
  "required": ["model"],
  "properties": {
    "model": {
      "type": "string",
      "title": "Model",
      "enum": [],
      "options": {
        "async": {
          "label": "List models",
          "action": "listModels"
        }
      }
    },
	"columns": {
      "type": "array",
      "title": "Model",
	  "items": {
		"enum": [],
		"type": "string"
	  },
      "options": {
        "async": {
          "label": "List columns",
          "action": "listColumns"
        }
      }
    }
  }
}
`)

	// Keboola is using enums with no options in the schema because the valid options are loaded dynamically.
	// Let's make sure that this does not cause the schema to be considered invalid.
	err := ValidateContent(schema, orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "parameters",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "model", Value: "example-model"},
				{Key: "columns", Value: []any{"column1", "column2"}},
			}),
		},
	}))
	require.NoError(t, err)
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

func TestValidateConfig_SkipTransformationComponents(t *testing.T) {
	t.Parallel()

	// Schema that requires "firstName" field
	schema := getTestSchema()

	// Content that violates the schema (missing required "firstName")
	invalidContent := orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "parameters",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "lastName", Value: "Brown"},
			}),
		},
	})

	// Test that validation is skipped for transformation components
	transformationComponents := []keboola.ComponentID{
		"keboola.python-transformation-v2",
		"keboola.snowflake-transformation",
		"keboola.google-bigquery-transformation",
	}

	for _, componentID := range transformationComponents {
		component := &keboola.Component{
			ComponentKey: keboola.ComponentKey{ID: componentID},
			Type:         "transformation",
			Name:         "Test Transformation",
			Schema:       schema,
		}
		config := &model.Config{Content: invalidContent}

		// Should return nil because validation is skipped for transformation components
		err := ValidateConfig(component, config)
		require.NoError(t, err, "validation should be skipped for component %s", componentID)
	}

	// Verify that a regular component still gets validated
	regularComponent := &keboola.Component{
		ComponentKey: keboola.ComponentKey{ID: "keboola.ex-generic"},
		Type:         "extractor",
		Name:         "Generic Extractor",
		Schema:       schema,
	}
	config := &model.Config{Content: invalidContent}
	err := ValidateConfig(regularComponent, config)
	require.Error(t, err, "validation should NOT be skipped for regular component")
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

func TestNormalizeSchema_OptionsDependencies_SingleValue(t *testing.T) {
	t.Parallel()

	// Schema with options.dependencies using a single value condition
	// This is the SFTP writer pattern: append_date_format is required only when append_date = 1
	schema := []byte(`
{
  "type": "object",
  "required": ["path", "append_date", "append_date_format"],
  "properties": {
    "path": {
      "type": "string"
    },
    "append_date": {
      "type": "integer",
      "default": 0
    },
    "append_date_format": {
      "type": "string",
      "options": {
        "dependencies": {
          "append_date": 1
        }
      }
    }
  }
}
`)

	// Normalize the schema
	normalizedSchema, err := NormalizeSchema(schema)
	require.NoError(t, err)

	// Verify the transformation: append_date_format should be removed from required
	// and an if/then construct should be added
	var result map[string]any
	err = json.Unmarshal(normalizedSchema, &result)
	require.NoError(t, err)

	// Check that append_date_format is removed from required
	required, ok := result["required"].([]any)
	require.True(t, ok)
	assert.Contains(t, required, "path")
	assert.Contains(t, required, "append_date")
	assert.NotContains(t, required, "append_date_format")

	// Check that allOf with if/then is added
	allOf, ok := result["allOf"].([]any)
	require.True(t, ok)
	require.Len(t, allOf, 1)

	// Validate the if/then structure
	ifThen, ok := allOf[0].(map[string]any)
	require.True(t, ok)
	ifClause, ok := ifThen["if"].(map[string]any)
	require.True(t, ok)
	ifProps, ok := ifClause["properties"].(map[string]any)
	require.True(t, ok)
	appendDateCond, ok := ifProps["append_date"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, 1, appendDateCond["const"], 0.001)

	thenClause, ok := ifThen["then"].(map[string]any)
	require.True(t, ok)
	thenRequired, ok := thenClause["required"].([]any)
	require.True(t, ok)
	assert.Contains(t, thenRequired, "append_date_format")
}

func TestNormalizeSchema_OptionsDependencies_ArrayValue(t *testing.T) {
	t.Parallel()

	// Schema with options.dependencies using an array value condition
	// This is the FTP writer pattern: passive_mode is required only when protocol is one of ["FTP", "Ex-FTPS", "Im-FTPS"]
	schema := []byte(`
{
  "type": "object",
  "required": ["protocol", "passive_mode"],
  "properties": {
    "protocol": {
      "type": "string",
      "enum": ["FTP", "Ex-FTPS", "Im-FTPS", "SFTP"]
    },
    "passive_mode": {
      "type": "boolean",
      "options": {
        "dependencies": {
          "protocol": ["FTP", "Ex-FTPS", "Im-FTPS"]
        }
      }
    }
  }
}
`)

	// Normalize the schema
	normalizedSchema, err := NormalizeSchema(schema)
	require.NoError(t, err)

	// Verify the transformation
	var result map[string]any
	err = json.Unmarshal(normalizedSchema, &result)
	require.NoError(t, err)

	// Check that passive_mode is removed from required
	required, ok := result["required"].([]any)
	require.True(t, ok)
	assert.Contains(t, required, "protocol")
	assert.NotContains(t, required, "passive_mode")

	// Check that allOf with if/then is added with enum condition
	allOf, ok := result["allOf"].([]any)
	require.True(t, ok)
	require.Len(t, allOf, 1)

	ifThen, ok := allOf[0].(map[string]any)
	require.True(t, ok)
	ifClause, ok := ifThen["if"].(map[string]any)
	require.True(t, ok)
	ifProps, ok := ifClause["properties"].(map[string]any)
	require.True(t, ok)
	protocolCond, ok := ifProps["protocol"].(map[string]any)
	require.True(t, ok)
	enumValues, ok := protocolCond["enum"].([]any)
	require.True(t, ok)
	assert.Contains(t, enumValues, "FTP")
	assert.Contains(t, enumValues, "Ex-FTPS")
	assert.Contains(t, enumValues, "Im-FTPS")
}

func TestNormalizeSchema_OptionsDependencies_MultipleFields(t *testing.T) {
	t.Parallel()

	// Schema with multiple fields having options.dependencies
	schema := []byte(`
{
  "type": "object",
  "required": ["mode", "field_a", "field_b"],
  "properties": {
    "mode": {
      "type": "string"
    },
    "field_a": {
      "type": "string",
      "options": {
        "dependencies": {
          "mode": "advanced"
        }
      }
    },
    "field_b": {
      "type": "string",
      "options": {
        "dependencies": {
          "mode": "expert"
        }
      }
    }
  }
}
`)

	// Normalize the schema
	normalizedSchema, err := NormalizeSchema(schema)
	require.NoError(t, err)

	// Verify the transformation
	var result map[string]any
	err = json.Unmarshal(normalizedSchema, &result)
	require.NoError(t, err)

	// Check that field_a and field_b are removed from required
	required, ok := result["required"].([]any)
	require.True(t, ok)
	assert.Contains(t, required, "mode")
	assert.NotContains(t, required, "field_a")
	assert.NotContains(t, required, "field_b")

	// Check that allOf has two if/then constructs
	allOf, ok := result["allOf"].([]any)
	require.True(t, ok)
	assert.Len(t, allOf, 2)
}

func TestNormalizeSchema_OptionsDependencies_NestedObject(t *testing.T) {
	t.Parallel()

	// Schema with options.dependencies in a nested object
	schema := []byte(`
{
  "type": "object",
  "properties": {
    "connection": {
      "type": "object",
      "required": ["type", "ssl_cert"],
      "properties": {
        "type": {
          "type": "string"
        },
        "ssl_cert": {
          "type": "string",
          "options": {
            "dependencies": {
              "type": "ssl"
            }
          }
        }
      }
    }
  }
}
`)

	// Normalize the schema
	normalizedSchema, err := NormalizeSchema(schema)
	require.NoError(t, err)

	// Verify the transformation
	var result map[string]any
	err = json.Unmarshal(normalizedSchema, &result)
	require.NoError(t, err)

	// Navigate to the nested connection object
	props, ok := result["properties"].(map[string]any)
	require.True(t, ok)
	connection, ok := props["connection"].(map[string]any)
	require.True(t, ok)

	// Check that ssl_cert is removed from required in the nested object
	required, ok := connection["required"].([]any)
	require.True(t, ok)
	assert.Contains(t, required, "type")
	assert.NotContains(t, required, "ssl_cert")

	// Check that allOf with if/then is added to the nested object
	allOf, ok := connection["allOf"].([]any)
	require.True(t, ok)
	require.Len(t, allOf, 1)
}

func TestNormalizeSchema_OptionsDependencies_NotInRequired(t *testing.T) {
	t.Parallel()

	// Schema with options.dependencies but the field is NOT in the required array.
	// options.dependencies is for UI visibility, NOT for making fields required.
	// Therefore, if the field was not originally required, we should NOT add if/then for it.
	schema := []byte(`
{
  "type": "object",
  "required": ["mode"],
  "properties": {
    "mode": {
      "type": "string"
    },
    "optional_field": {
      "type": "string",
      "options": {
        "dependencies": {
          "mode": "advanced"
        }
      }
    }
  }
}
`)

	// Normalize the schema
	normalizedSchema, err := NormalizeSchema(schema)
	require.NoError(t, err)

	// Verify the transformation
	var result map[string]any
	err = json.Unmarshal(normalizedSchema, &result)
	require.NoError(t, err)

	// Check that required still only contains "mode"
	required, ok := result["required"].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"mode"}, required)

	// Check that NO allOf was added - optional_field was not in required,
	// so we don't need conditional requirement logic
	_, hasAllOf := result["allOf"]
	assert.False(t, hasAllOf, "allOf should NOT be added for fields that were not originally required")
}

func TestNormalizeSchema_OptionsDependencies_Validation(t *testing.T) {
	t.Parallel()

	// Test that the transformed schema validates correctly
	schema := []byte(`
{
  "type": "object",
  "required": ["append_date", "append_date_format"],
  "properties": {
    "append_date": {
      "type": "integer",
      "default": 0
    },
    "append_date_format": {
      "type": "string",
      "options": {
        "dependencies": {
          "append_date": 1
        }
      }
    }
  }
}
`)

	// Test 1: When append_date = 0, append_date_format should NOT be required
	content1 := orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "parameters",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "append_date", Value: 0},
			}),
		},
	})
	err := ValidateContent(schema, content1)
	require.NoError(t, err, "Should pass validation when append_date=0 and append_date_format is missing")

	// Test 2: When append_date = 1, append_date_format SHOULD be required
	content2 := orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "parameters",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "append_date", Value: 1},
			}),
		},
	})
	err = ValidateContent(schema, content2)
	require.Error(t, err, "Should fail validation when append_date=1 and append_date_format is missing")
	assert.Contains(t, err.Error(), "append_date_format")

	// Test 3: When append_date = 1 and append_date_format is provided, should pass
	content3 := orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "parameters",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "append_date", Value: 1},
				{Key: "append_date_format", Value: "Y-m-d"},
			}),
		},
	})
	err = ValidateContent(schema, content3)
	require.NoError(t, err, "Should pass validation when append_date=1 and append_date_format is provided")
}

func TestNormalizeSchema_OptionsDependencies_ArrayValidation(t *testing.T) {
	t.Parallel()

	// Test that array-based dependencies validate correctly
	schema := []byte(`
{
  "type": "object",
  "required": ["protocol", "passive_mode"],
  "properties": {
    "protocol": {
      "type": "string"
    },
    "passive_mode": {
      "type": "boolean",
      "options": {
        "dependencies": {
          "protocol": ["FTP", "FTPS"]
        }
      }
    }
  }
}
`)

	// Test 1: When protocol = "SFTP", passive_mode should NOT be required
	content1 := orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "parameters",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "protocol", Value: "SFTP"},
			}),
		},
	})
	err := ValidateContent(schema, content1)
	require.NoError(t, err, "Should pass validation when protocol=SFTP and passive_mode is missing")

	// Test 2: When protocol = "FTP", passive_mode SHOULD be required
	content2 := orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "parameters",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "protocol", Value: "FTP"},
			}),
		},
	})
	err = ValidateContent(schema, content2)
	require.Error(t, err, "Should fail validation when protocol=FTP and passive_mode is missing")
	assert.Contains(t, err.Error(), "passive_mode")

	// Test 3: When protocol = "FTP" and passive_mode is provided, should pass
	content3 := orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "parameters",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "protocol", Value: "FTP"},
				{Key: "passive_mode", Value: true},
			}),
		},
	})
	err = ValidateContent(schema, content3)
	require.NoError(t, err, "Should pass validation when protocol=FTP and passive_mode is provided")
}

func TestNormalizeSchema_ExistingAllOfIfThen(t *testing.T) {
	t.Parallel()

	// Test that schemas with existing allOf containing if/then/else constructs
	// continue to work correctly (backward compatibility)
	schema := []byte(`
{
  "type": "object",
  "required": ["append_date"],
  "properties": {
    "append_date": {
      "type": "integer",
      "default": 0
    },
    "append_date_format": {
      "type": "string"
    }
  },
  "allOf": [{
    "if": { "properties": { "append_date": { "const": 1 } } },
    "then": { "required": ["append_date_format"] }
  }]
}
`)

	// Test 1: When append_date = 0, append_date_format should NOT be required
	content1 := orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "parameters",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "append_date", Value: 0},
			}),
		},
	})
	err := ValidateContent(schema, content1)
	require.NoError(t, err, "Should pass validation when append_date=0 and append_date_format is missing")

	// Test 2: When append_date = 1, append_date_format SHOULD be required
	content2 := orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "parameters",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "append_date", Value: 1},
			}),
		},
	})
	err = ValidateContent(schema, content2)
	require.Error(t, err, "Should fail validation when append_date=1 and append_date_format is missing")
	assert.Contains(t, err.Error(), "append_date_format")

	// Test 3: When append_date = 1 and append_date_format is provided, should pass
	content3 := orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "parameters",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "append_date", Value: 1},
				{Key: "append_date_format", Value: "Y-m-d"},
			}),
		},
	})
	err = ValidateContent(schema, content3)
	require.NoError(t, err, "Should pass validation when append_date=1 and append_date_format is provided")
}

func TestNormalizeSchema_OptionsDependencies_MissingDependencyField(t *testing.T) {
	t.Parallel()

	// This test uses the actual schema from kds-team.app-orchestration-trigger-queue-v2 component.
	// triggerActionOnFailure is NOT in the required array.
	// triggerActionOnFailure has options.dependencies: { waitUntilFinish: true }
	// actionOnFailureSettings has options.dependencies: { triggerActionOnFailure: true }
	schema := []byte(`
{
  "type": "object",
  "required": ["waitUntilFinish", "#kbcToken", "kbcUrl", "orchestrationId"],
  "properties": {
    "waitUntilFinish": {
      "type": "boolean"
    },
    "triggerActionOnFailure": {
      "type": "boolean",
      "options": {
        "dependencies": {
          "waitUntilFinish": true
        }
      }
    },
    "actionOnFailureSettings": {
      "type": "object",
      "required": ["failureConfigurationId"],
      "properties": {
        "failureConfigurationId": {
          "type": "string"
        }
      },
      "options": {
        "dependencies": {
          "triggerActionOnFailure": true
        }
      }
    },
    "#kbcToken": {
      "type": "string"
    },
    "kbcUrl": {
      "type": "string"
    },
    "orchestrationId": {
      "type": "string"
    }
  }
}
`)

	// Test 1: triggerActionOnFailure field is missing entirely (fire-and-forget config)
	// This should PASS - actionOnFailureSettings should NOT be required
	content1 := orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "parameters",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "waitUntilFinish", Value: false},
				{Key: "#kbcToken", Value: "xxx"},
				{Key: "kbcUrl", Value: "-"},
				{Key: "orchestrationId", Value: "123"},
				// triggerActionOnFailure is NOT present at all
			}),
		},
	})
	err := ValidateContent(schema, content1)
	require.NoError(t, err, "Should pass when triggerActionOnFailure field is missing (undefined)")

	// Test 2: triggerActionOnFailure is explicitly false
	// This should PASS - actionOnFailureSettings should NOT be required
	content2 := orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "parameters",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "waitUntilFinish", Value: true},
				{Key: "#kbcToken", Value: "xxx"},
				{Key: "kbcUrl", Value: "-"},
				{Key: "triggerActionOnFailure", Value: false},
				{Key: "orchestrationId", Value: "123"},
			}),
		},
	})
	err = ValidateContent(schema, content2)
	require.NoError(t, err, "Should pass when triggerActionOnFailure is explicitly false")

	// Test 3: triggerActionOnFailure is true but actionOnFailureSettings is missing
	// This should PASS - actionOnFailureSettings was NOT in the required array,
	// options.dependencies is only for UI visibility, not for making fields required.
	content3 := orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "parameters",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "waitUntilFinish", Value: true},
				{Key: "#kbcToken", Value: "xxx"},
				{Key: "kbcUrl", Value: "-"},
				{Key: "triggerActionOnFailure", Value: true},
				{Key: "orchestrationId", Value: "123"},
			}),
		},
	})
	err = ValidateContent(schema, content3)
	require.NoError(t, err, "Should pass - actionOnFailureSettings was NOT in required, options.dependencies is for visibility only")

	// Test 4: triggerActionOnFailure is true and actionOnFailureSettings is provided but missing internal required field
	// This should FAIL - failureConfigurationId is required WITHIN actionOnFailureSettings
	content4 := orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "parameters",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "waitUntilFinish", Value: true},
				{Key: "#kbcToken", Value: "xxx"},
				{Key: "kbcUrl", Value: "-"},
				{Key: "triggerActionOnFailure", Value: true},
				{Key: "orchestrationId", Value: "123"},
				{
					Key:   "actionOnFailureSettings",
					Value: orderedmap.FromPairs([]orderedmap.Pair{
						// missing failureConfigurationId
					}),
				},
			}),
		},
	})
	err = ValidateContent(schema, content4)
	require.Error(t, err, "Should fail - failureConfigurationId is required within actionOnFailureSettings")
	assert.Contains(t, err.Error(), "failureConfigurationId")

	// Test 5: triggerActionOnFailure is true and actionOnFailureSettings is provided with required field
	// This should PASS
	content5 := orderedmap.FromPairs([]orderedmap.Pair{
		{
			Key: "parameters",
			Value: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "waitUntilFinish", Value: true},
				{Key: "#kbcToken", Value: "xxx"},
				{Key: "kbcUrl", Value: "-"},
				{Key: "triggerActionOnFailure", Value: true},
				{Key: "orchestrationId", Value: "123"},
				{
					Key: "actionOnFailureSettings",
					Value: orderedmap.FromPairs([]orderedmap.Pair{
						{Key: "failureConfigurationId", Value: "456"},
					}),
				},
			}),
		},
	})
	err = ValidateContent(schema, content5)
	require.NoError(t, err, "Should pass when actionOnFailureSettings is provided with required fields")
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
