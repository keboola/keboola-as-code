package schema

import (
	"encoding/json"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

type propertyOrderSchema int64

func buildPropertyOrderVocabulary() (*jsonschema.Vocabulary, error) {
	url := "propertyOrder.json"
	schemaString := `
{
  "properties" : {
    "propertyOrder": {
      "type": "integer"
    }
  }
}
`

	schema, err := jsonschema.UnmarshalJSON(strings.NewReader(schemaString))
	if err != nil {
		return nil, err
	}

	c := jsonschema.NewCompiler()
	if err := c.AddResource(url, schema); err != nil {
		return nil, err
	}
	sch, err := c.Compile(url)
	if err != nil {
		return nil, err
	}

	return &jsonschema.Vocabulary{
		URL:     url,
		Schema:  sch,
		Compile: compilePropertyOrder,
	}, nil
}

func compilePropertyOrder(ctx *jsonschema.CompilerContext, obj map[string]any) (jsonschema.SchemaExt, error) {
	if value, ok := obj["propertyOrder"]; ok {
		n, err := value.(json.Number).Int64()
		return propertyOrderSchema(n), err
	}

	// nothing to compile, return nil
	return nil, nil
}

func (s propertyOrderSchema) Validate(_ *jsonschema.ValidatorContext, _ any) {
}
