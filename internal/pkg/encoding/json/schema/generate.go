package schema

import (
	"math"
	"sort"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

func GenerateDocument(schemaDef []byte) (*orderedmap.OrderedMap, error) {
	// Is schema empty?
	if len(schemaDef) == 0 {
		return orderedmap.New(), nil
	}

	// Compile schema
	schema, err := compileSchema(schemaDef, true)
	if err != nil {
		return nil, err
	}

	content := getDefaultValueFor(schema, 0).(*orderedmap.OrderedMap)
	return content, nil
}

func getDefaultValueFor(schema *jsonschema.Schema, level int) any {
	// Return default value
	if schema.Default != nil {
		return schema.Default
	}

	// Return default value
	if schema.Enum != nil && len(schema.Enum.Values) > 0 {
		return schema.Enum.Values[0]
	}

	// Prevent infinite recursion
	if level > 20 {
		return ``
	}

	// Reference
	if schema.Ref != nil {
		return getDefaultValueFor(schema.Ref, level+1)
	}

	// Process nested schemas
	if v := getFirstChildSchema(schema.OneOf); v != nil {
		return getDefaultValueFor(v, level+1)
	}
	if len(schema.AllOf) > 0 {
		return mergeDefaultValues(schema.AllOf, level+1)
	}
	if len(schema.AnyOf) > 0 {
		return mergeDefaultValues(schema.AnyOf, level+1)
	}

	// Generate value based on type
	firstType := getFirstType(schema)
	switch firstType {
	case `array`:
		// Generate empty array by default
		// Functions like ensureMinimalBlocks handle adding required items for specific use cases
		values := make([]any, 0)
		switch v := schema.Items.(type) {
		case *jsonschema.Schema:
			// Don't add any items - keep array empty
		case []*jsonschema.Schema:
			for _, item := range v {
				values = append(values, getDefaultValueFor(item, level+1))
			}
		}
		return values
	case `object`:
		return buildOrderedMap(schema, level)
	case `string`:
		if schema.Format == nil {
			return ``
		}

		switch schema.Format.Name {
		case `date-time`:
			return `2018-11-13T20:20:39+00:00`
		case `time`:
			return `20:20:39+00:00`
		case `date`:
			return `2018-11-13`
		case `duration`:
			return `P3D`
		case `email`:
			return `user@company.com`
		case `idn-email`:
			return `user@company.com`
		case `uuid`:
			return `3e4666bf-d5e5-4aa7-b8ce-cefe41c7568a`
		}
		return ``
	case `number`, `integer`:
		return 0
	case `boolean`:
		return false
	default:
		if level == 0 {
			return buildOrderedMap(schema, level)
		}

		return ``
	}
}

func buildOrderedMap(schema *jsonschema.Schema, level int) any {
	values := orderedmap.New()
	if schema.Properties != nil {
		props := make([]*jsonschema.Schema, 0)
		keys := make(map[string]string)
		for key, prop := range schema.Properties {
			props = append(props, prop)
			keys[prop.Location] = key
		}
		sortSchemas(props)

		for _, prop := range props {
			key := keys[prop.Location]
			values.Set(key, getDefaultValueFor(prop, level+1))
		}
	}
	return values
}

func getFirstType(schema *jsonschema.Schema) string {
	if schema.Types != nil && !schema.Types.IsEmpty() {
		return schema.Types.ToStrings()[0]
	}
	return `unknown`
}

func getFirstChildSchema(schemas []*jsonschema.Schema) *jsonschema.Schema {
	if len(schemas) > 0 {
		return schemas[0]
	}

	// Not found
	return nil
}

func sortSchemas(schemas []*jsonschema.Schema) {
	sort.Slice(schemas, func(i, j int) bool {
		// Sort by "propertyOrder" key if present
		orderI := getPropertyOrder(schemas[i])
		orderJ := getPropertyOrder(schemas[j])
		if orderI != orderJ {
			return orderI < orderJ
		}
		// Otherwise alphabetically
		return schemas[i].Location < schemas[j].Location
	})
}

func getPropertyOrder(schema *jsonschema.Schema) int64 {
	for _, extension := range schema.Extensions {
		if propertyOrder, ok := extension.(propertyOrderSchema); ok {
			return int64(propertyOrder)
		}
	}
	return math.MaxInt64
}

func mergeDefaultValues(schemas []*jsonschema.Schema, level int) any {
	// No schema
	if len(schemas) == 0 {
		return ``
	}

	// Multiple schemas, are there some objects?
	values := orderedmap.New()
	for _, schema := range schemas {
		def := getDefaultValueFor(schema, level)
		if m, ok := def.(*orderedmap.OrderedMap); ok {
			for _, k := range m.Keys() {
				v, _ := m.Get(k)
				values.Set(k, v)
			}
		}
	}

	// Found some object keys -> return
	if len(values.Keys()) > 0 {
		return values
	}

	// No object keys found -> get default value from the first schema
	return getDefaultValueFor(schemas[0], level)
}
