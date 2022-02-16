package schema_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestFieldMeta_Empty(t *testing.T) {
	t.Parallel()
	meta, found, err := schema.FieldMeta([]byte(""), orderedmap.Key{})
	assert.Empty(t, meta)
	assert.False(t, found)
	assert.Nil(t, err)
}

func TestFieldMeta_Complex(t *testing.T) {
	t.Parallel()
	componentSchema := `
{
  "type": "object",
  "title": "Configuration Parameters",
  "properties": {
    "db": {
      "type": "object",
      "title": "Database",
      "required": [
        "#connectionString"
      ],
      "properties": {
        "#connectionString": {
          "type": "string",
          "title": "Connection String",
          "default": "",
          "minLength": 1,
          "description": "Eg. \"DefaultEndpointsProtocol=https;...\". The value will be encrypted when saved.",
          "propertyOrder": 1
        },
        "limit": {
          "type": "integer",
          "title": "Query Limit",
          "default": 1234,
          "propertyOrder": 1
        }
      }
    }
  }
}
`
	// Not found, empty path
	meta, found, err := schema.FieldMeta([]byte(componentSchema), orderedmap.Key{})
	assert.Empty(t, meta)
	assert.False(t, found)
	assert.Nil(t, err)

	// Not found
	meta, found, err = schema.FieldMeta([]byte(componentSchema), orderedmap.KeyFromStr("foo.bar"))
	assert.Empty(t, meta)
	assert.False(t, found)
	assert.Nil(t, err)

	// Found object
	meta, found, err = schema.FieldMeta([]byte(componentSchema), orderedmap.KeyFromStr("properties.db"))
	assert.NotEmpty(t, meta)
	assert.True(t, found)
	assert.Nil(t, err)
	assert.Equal(t, "Database", meta.Title)
	assert.Equal(t, "", meta.Description)
	assert.Equal(t, nil, meta.Default)
	assert.False(t, meta.Required)

	// Found string, required field
	meta, found, err = schema.FieldMeta([]byte(componentSchema), orderedmap.KeyFromStr("properties.db.#connectionString"))
	assert.NotEmpty(t, meta)
	assert.True(t, found)
	assert.Nil(t, err)
	assert.Equal(t, "Connection String", meta.Title)
	assert.Equal(t, `Eg. "DefaultEndpointsProtocol=https;...". The value will be encrypted when saved.`, meta.Description)
	assert.Equal(t, nil, meta.Default)
	assert.True(t, meta.Required)

	// Found int, default field
	meta, found, err = schema.FieldMeta([]byte(componentSchema), orderedmap.KeyFromStr("properties.db.limit"))
	assert.NotEmpty(t, meta)
	assert.True(t, found)
	assert.Nil(t, err)
	assert.Equal(t, "Query Limit", meta.Title)
	assert.Equal(t, "", meta.Description)
	assert.Equal(t, "1234", meta.Default)
	assert.False(t, meta.Required)
}
