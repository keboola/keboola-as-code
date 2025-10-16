package schema

import (
	"slices"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/spf13/cast"
)

type FieldMetadata struct {
	Title       string
	Description string
	Default     any
	Required    bool
}

func FieldMeta(schemaDef []byte, path orderedmap.Path) (out FieldMetadata, found bool, err error) {
	// Is schema empty?
	if len(schemaDef) == 0 {
		return out, false, nil
	}

	// Compile schema
	schema, err := compileSchema(schemaDef, true)
	if err != nil {
		return out, false, err
	}

	// Search for field
	out, found = getFieldMeta(schema, path)
	return out, found, err
}

func getFieldMeta(schema *jsonschema.Schema, path orderedmap.Path) (out FieldMetadata, found bool) {
	// Skip first step: component schema starts at "properties"
	if path.First() != orderedmap.MapStep("parameters") {
		return out, false
	}
	path = path.WithoutFirst()

	// Get field
	parent, field := getField(schema, path)
	if field == nil {
		return out, false
	}

	// Get title and description
	out.Title = field.Title
	out.Description = field.Description

	// Default value, for example json.Number, convert it to string
	defaultVal := cast.ToString(field.Default)
	if defaultVal != "" {
		out.Default = defaultVal
	}

	// Detect required field
	lastStep, _ := path.Last().(orderedmap.MapStep)
	if parent != nil {
		if slices.Contains(parent.Required, lastStep.Key()) {
			out.Required = true
		}
	}
	return out, true
}

func getField(current *jsonschema.Schema, path orderedmap.Path) (parent *jsonschema.Schema, field *jsonschema.Schema) {
	lastIndex := len(path) - 1
	for index, step := range path {
		// Only object keys are supported
		if step, ok := step.(orderedmap.MapStep); ok && getFirstType(current) == "object" {
			for key, nested := range current.Properties {
				if key == step.Key() {
					parent, current = current, nested
					if index == lastIndex {
						// Found
						return parent, current
					} else {
						// Next step
						break
					}
				}
			}
		}
	}

	return nil, nil
}
