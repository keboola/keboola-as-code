package input

import (
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestFind(t *testing.T) {
	t.Parallel()
	objectKey := model.ConfigKey{BranchID: 123, ComponentID: "keboola.foo-bar", ID: "456"}
	component := &keboola.Component{
		ComponentKey: keboola.ComponentKey{ID: "keboola.foo-bar"},
	}
	contentJSON := `
{
  "storage": {
    "foo": "bar"
  },
 "parameters": {
    "object": {
      "array": [
        123,
        {
          "string": "Lorem ipsum dolor",
          "#password": "my password",
          "int": 123,
          "double": 78.90,
          "bool": false
        }
      ]
    }
  }
}
`
	content := orderedmap.New()
	json.MustDecodeString(contentJSON, content)

	// Check
	results := Find(objectKey, component, content)
	assert.Equal(t, []ObjectField{
		{
			Input: Input{
				ID:      "foo-bar-object-array-1-string",
				Name:    "Object Array String",
				Type:    TypeString,
				Kind:    KindInput,
				Default: "Lorem ipsum dolor",
			},
			ObjectKey: objectKey,
			Path:      orderedmap.PathFromStr("parameters.object.array[1].string"),
			Example:   "Lorem ipsum dolor",
			Selected:  false,
		},
		{
			Input: Input{
				ID:   "foo-bar-object-array-1-password",
				Name: "Object Array Password",
				Type: TypeString,
				Kind: KindHidden,
			},
			ObjectKey: objectKey,
			Path:      orderedmap.PathFromStr("parameters.object.array[1].#password"),
			Example:   "",
			Selected:  true,
		},
		{
			Input: Input{
				ID:      "foo-bar-object-array-1-int",
				Name:    "Object Array Int",
				Type:    TypeInt,
				Kind:    KindInput,
				Default: 123,
			},
			ObjectKey: objectKey,
			Path:      orderedmap.PathFromStr("parameters.object.array[1].int"),
			Example:   "123",
			Selected:  false,
		},
		{
			Input: Input{
				ID:      "foo-bar-object-array-1-double",
				Name:    "Object Array Double",
				Type:    TypeDouble,
				Kind:    KindInput,
				Default: 78.9,
			},
			ObjectKey: objectKey,
			Path:      orderedmap.PathFromStr("parameters.object.array[1].double"),
			Example:   "78.9",
			Selected:  false,
		},
		{
			Input: Input{
				ID:      "foo-bar-object-array-1-bool",
				Name:    "Object Array Bool",
				Type:    TypeBool,
				Kind:    KindConfirm,
				Default: false,
			},
			ObjectKey: objectKey,
			Path:      orderedmap.PathFromStr("parameters.object.array[1].bool"),
			Example:   "false",
			Selected:  false,
		},
	}, results)
}

func TestFind_ComponentSchema(t *testing.T) {
	t.Parallel()

	schema := `{
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
        }
      }
    }
  }
}`

	objectKey := model.ConfigKey{BranchID: 123, ComponentID: "keboola.foo-bar", ID: "456"}
	component := &keboola.Component{
		ComponentKey: keboola.ComponentKey{ID: "keboola.foo-bar"},
		Schema:       json.RawMessage(schema),
	}
	contentJSON := `
{
  "storage": {
    "foo": "bar"
  },
 "parameters": {
    "db": {
      "#connectionString": "my-value"
    }
  }
}
`
	content := orderedmap.New()
	json.MustDecodeString(contentJSON, content)

	// Check
	results := Find(objectKey, component, content)
	assert.Equal(t, []ObjectField{
		{
			Input: Input{
				ID:          "foo-bar-db-connection-string",
				Name:        "Connection String",
				Description: `Eg. "DefaultEndpointsProtocol=https;...". The value will be encrypted when saved.`,
				Type:        TypeString,
				Kind:        KindHidden,
			},
			ObjectKey: objectKey,
			Path:      orderedmap.PathFromStr("parameters.db.#connectionString"),
			Example:   "",
			Selected:  true,
		},
	}, results)
}
