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
	assert.NoError(t, validateJsonSchema(schema, content))
}

func TestValidateJsonSchemaErr(t *testing.T) {
	schema := getTestSchema()
	parameters := utils.PairsToOrderedMap([]utils.Pair{
		{Key: "lastName", Value: "Brown"},
		{Key: "age", Value: -1},
	})
	content := utils.NewOrderedMap()
	content.Set(`parameters`, *parameters)
	err := validateJsonSchema(schema, content)
	assert.Error(t, err)
	expectedErr := `
- firstName is required
- age: Must be greater than or equal to 0
`
	assert.Equal(t, strings.TrimSpace(expectedErr), err.Error())
}

func TestValidateJsonSchemaSkipEmpty(t *testing.T) {
	schema := getTestSchema()
	content := utils.NewOrderedMap()
	assert.NoError(t, validateJsonSchema(schema, content))
}

func TestValidateJsonSchemaSkipEmptyParameters(t *testing.T) {
	schema := getTestSchema()
	content := utils.NewOrderedMap()
	content.Set(`parameters`, *utils.NewOrderedMap())
	assert.NoError(t, validateJsonSchema(schema, content))
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
      "type": "string"
    },
    "age": {
      "type": "integer",
      "minimum": 0
    }
  }
}
`
	schema := make(map[string]interface{})
	json.MustDecodeString(schemaJson, &schema)
	return schema
}
