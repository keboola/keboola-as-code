package schema

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestValidateJsonSchemaOk(t *testing.T) {
	schema := getTestSchema()
	parameters := utils.PairsToOrderedMap([]utils.Pair{
		{Key: "firstName", Value: "John"},
		{Key: "lastName", Value: "Brown"},
		{Key: "age", Value: 25},
	})
	content := utils.NewOrderedMap()
	content.Set(`parameters`, *parameters)
	assert.NoError(t, validateContent(schema, content))
}

func TestValidateJsonSchemaErr(t *testing.T) {
	schema := getTestSchema()
	parameters := utils.PairsToOrderedMap([]utils.Pair{
		{Key: "lastName", Value: "Brown"},
		{Key: "age", Value: -1},
		{
			Key: "address",
			Value: utils.PairsToOrderedMap([]utils.Pair{
				{Key: "number", Value: "abc"},
			}),
		},
	})
	content := utils.NewOrderedMap()
	content.Set(`parameters`, *parameters)
	err := validateContent(schema, content)
	assert.Error(t, err)
	expectedErr := `
- "firstName" value is required
- "address": "street" value is required
- "address.number": type should be integer, got string
- "age": must be greater than or equal to 0
`
	assert.Equal(t, strings.TrimSpace(expectedErr), err.Error())
}

func TestValidateJsonSchemaSkipEmpty(t *testing.T) {
	schema := getTestSchema()
	content := utils.NewOrderedMap()
	assert.NoError(t, validateContent(schema, content))
}

func TestValidateJsonSchemaSkipEmptyParameters(t *testing.T) {
	schema := getTestSchema()
	content := utils.NewOrderedMap()
	content.Set(`parameters`, *utils.NewOrderedMap())
	assert.NoError(t, validateContent(schema, content))
}

func getTestSchema() map[string]interface{} {
	schemaJson := `
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
`
	schema := make(map[string]interface{})
	json.MustDecodeString(schemaJson, &schema)
	return schema
}
