package schema

import (
	"encoding/json"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

// propertyOrderExt - jsonschema extension to store "propertyOrder" key.
type propertyOrderExt struct{}

type propertyOrderSchema int64

func registerPropertyOrderExt(c *jsonschema.Compiler) {
	c.RegisterExtension("propertyOrder", propertyOrderMeta(), propertyOrderExt{})
}

func propertyOrderMeta() *jsonschema.Schema {
	schema := `
{
  "properties" : {
    "propertyOrder": {
    "type": "integer"
    }
  }
}
`
	return jsonschema.MustCompileString("propertyOrder.json", schema)
}

func (propertyOrderExt) Compile(_ jsonschema.CompilerContext, m map[string]interface{}) (jsonschema.ExtSchema, error) {
	if value, ok := m["propertyOrder"]; ok {
		n, err := value.(json.Number).Int64()
		return propertyOrderSchema(n), err
	}

	// nothing to compile, return nil
	return nil, nil
}

func (s propertyOrderSchema) Validate(_ jsonschema.ValidationContext, _ interface{}) error {
	return nil
}
