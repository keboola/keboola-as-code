package input

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestFind(t *testing.T) {
	t.Parallel()
	objectKey := model.ConfigKey{BranchId: 123, ComponentId: "keboola.foo-bar", Id: "456"}
	componentKey := model.ComponentKey{Id: "keboola.foo-bar"}
	contentJson := `
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
	json.MustDecodeString(contentJson, content)

	// Run
	results := Find(objectKey, componentKey, content)
	assert.Equal(t, []ObjectField{
		{
			Input: Input{
				Id:      "foo-bar-object-array-1-string",
				Name:    "",
				Type:    TypeString,
				Kind:    KindInput,
				Default: "Lorem ipsum dolor",
			},
			ObjectKey: objectKey,
			Path:      orderedmap.KeyFromStr("parameters.object.array[1].string"),
			Example:   "Lorem ipsum dolor",
			Selected:  false,
		},
		{
			Input: Input{
				Id:   "foo-bar-object-array-1-password",
				Name: "",
				Type: TypeString,
				Kind: KindHidden,
			},
			ObjectKey: objectKey,
			Path:      orderedmap.KeyFromStr("parameters.object.array[1].#password"),
			Example:   "",
			Selected:  true,
		},
		{
			Input: Input{
				Id:      "foo-bar-object-array-1-int",
				Name:    "",
				Type:    TypeInt,
				Kind:    KindInput,
				Default: 123,
			},
			ObjectKey: objectKey,
			Path:      orderedmap.KeyFromStr("parameters.object.array[1].int"),
			Example:   "123",
			Selected:  false,
		},
		{
			Input: Input{
				Id:      "foo-bar-object-array-1-double",
				Name:    "",
				Type:    TypeDouble,
				Kind:    KindInput,
				Default: 78.9,
			},
			ObjectKey: objectKey,
			Path:      orderedmap.KeyFromStr("parameters.object.array[1].double"),
			Example:   "78.9",
			Selected:  false,
		},
		{
			Input: Input{
				Id:      "foo-bar-object-array-1-bool",
				Name:    "",
				Type:    TypeBool,
				Kind:    KindConfirm,
				Default: false,
			},
			ObjectKey: objectKey,
			Path:      orderedmap.KeyFromStr("parameters.object.array[1].bool"),
			Example:   "false",
			Selected:  false,
		},
	}, results)
}
