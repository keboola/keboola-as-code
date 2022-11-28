package schema

import (
	"strings"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
)

func TestValidateJSONSchemaOk(t *testing.T) {
	t.Parallel()
	schema := getTestSchema()
	parameters := orderedmap.FromPairs([]orderedmap.Pair{
		{Key: "firstName", Value: "John"},
		{Key: "lastName", Value: "Brown"},
		{Key: "age", Value: 25},
	})
	content := orderedmap.New()
	content.Set(`parameters`, parameters)
	assert.NoError(t, validateContent(schema, content))
}

func TestValidateJSONSchemaErr(t *testing.T) {
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
	err := validateContent(schema, content)
	assert.Error(t, err)
	expectedErr := `
- missing properties: "firstName"
- "address": missing properties: "street"
- "address.number": expected integer, but got string
- "age": must be >= 0 but found -1
`
	assert.Equal(t, strings.TrimSpace(expectedErr), err.Error())
}

func TestValidateJSONSchemaSkipEmpty(t *testing.T) {
	t.Parallel()
	schema := getTestSchema()
	content := orderedmap.New()
	assert.NoError(t, validateContent(schema, content))
}

func TestValidateJSONSchemaSkipEmptyParameters(t *testing.T) {
	t.Parallel()
	schema := getTestSchema()
	content := orderedmap.New()
	content.Set(`parameters`, orderedmap.New())
	assert.NoError(t, validateContent(schema, content))
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
