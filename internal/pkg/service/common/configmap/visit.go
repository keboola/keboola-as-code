package configmap

import (
	"reflect"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type VisitConfig struct {
	// OnField maps field to a custom field name, for example from a tag.
	// If ok == false, then the field is ignored.
	OnField func(field reflect.StructField, path orderedmap.Path) (fieldName string, ok bool)
	// OnValue is called on each field
	OnValue func(vc *VisitContext) error
}

type VisitContext struct {
	// StructField contains metadata about the structure field, for example tags.
	StructField reflect.StructField
	// OriginalPath is path to the filed in the source structure.
	OriginalPath orderedmap.Path
	// MappedPath is path to the field modified by the VisitConfig.OnField function.
	MappedPath orderedmap.Path
	// Value is untouched Value of the structure field.
	Value reflect.Value
	// PrimitiveValue is Value converted to base/primitive type, if it is possible,
	// otherwise the PrimitiveValue is same as the Value.
	PrimitiveValue reflect.Value
	// Type of the Value
	Type reflect.Type
	// Leaf is true if it is the last value in the path.
	Leaf bool
	// Sensitive is true, if the field has `sensitive:"true"` tag.
	Sensitive bool
	// Usage contains value from the "configUsage" tag, if any.
	Usage string
	// Validate contains value from the "validate" tag, if any.
	Validate string
}

// Visit each nested structure field.
func Visit(value reflect.Value, cfg VisitConfig) error {
	vc := &VisitContext{}
	vc.Value = value
	vc.PrimitiveValue = value
	vc.Type = value.Type()
	return doVisit(vc, cfg)
}

func doVisit(vc *VisitContext, cfg VisitConfig) error {
	onLeaf := func(primitiveValue reflect.Value) error {
		vc.PrimitiveValue = primitiveValue
		vc.Leaf = true
		return cfg.OnValue(vc)
	}

	// Handle text
	text, err := MarshaText(vc.Type, vc.Value)
	switch {
	case errors.As(err, &NoTextTypeError{}):
		// continue, no marshaller found
	case err != nil:
		// marshaller found, but an error occurred
		return err
	default:
		// ok
		return onLeaf(reflect.ValueOf(string(text)))
	}

	// Dereference pointer, if any
	typ := vc.Type
	value := vc.Value
	if typ.Kind() == reflect.Pointer {
		if value.IsValid() {
			value = value.Elem()
		} else {
			value = reflect.Zero(typ.Elem())
		}
		typ = typ.Elem()
	}

	// Handle structure
	if typ.Kind() == reflect.Struct {
		// Call callback
		if err := cfg.OnValue(vc); err != nil {
			return err
		}

		for i := 0; i < typ.NumField(); i++ {
			// Fill context with field information
			field := &VisitContext{}
			field.StructField = typ.Field(i)
			field.OriginalPath = append(field.OriginalPath, vc.OriginalPath...)
			field.OriginalPath = append(field.OriginalPath, orderedmap.MapStep(field.StructField.Name))
			field.MappedPath = vc.MappedPath
			field.Type = field.StructField.Type
			field.Leaf = false
			if value.IsValid() {
				fv := value.Field(i)
				field.Value = fv
				field.PrimitiveValue = fv
			}

			// Mark field and all its children as sensitive according to the tag
			field.Sensitive = vc.Sensitive || field.StructField.Tag.Get(sensitiveTag) == "true"

			// Set usage from the tag, or use parent usage text
			field.Usage = vc.Usage
			if usage := field.StructField.Tag.Get(configUsageTag); usage != "" {
				field.Usage = usage
			}

			// Set validate from the tag
			if validate := field.StructField.Tag.Get(validateTag); validate != "" {
				field.Validate = validate
			}

			// Map field name, ignore skipped fields
			if fieldName, ok := cfg.OnField(field.StructField, field.OriginalPath); !ok {
				continue
			} else if fieldName != "" {
				field.MappedPath = append(field.MappedPath, orderedmap.MapStep(fieldName))
			}

			// Step down
			if err := doVisit(field, cfg); err != nil {
				return err
			}
		}

		return nil
	}

	// Handle base types
	switch value.Kind() {
	case reflect.Int:
		return onLeaf(reflect.ValueOf(int(value.Int())))
	case reflect.Int8:
		return onLeaf(reflect.ValueOf(int8(value.Int())))
	case reflect.Int16:
		return onLeaf(reflect.ValueOf(int16(value.Int())))
	case reflect.Int32:
		return onLeaf(reflect.ValueOf(int32(value.Int())))
	case reflect.Int64:
		return onLeaf(reflect.ValueOf(value.Int()))
	case reflect.Uint:
		return onLeaf(reflect.ValueOf(uint(value.Uint())))
	case reflect.Uint8:
		return onLeaf(reflect.ValueOf(uint8(value.Uint())))
	case reflect.Uint16:
		return onLeaf(reflect.ValueOf(uint16(value.Uint())))
	case reflect.Uint32:
		return onLeaf(reflect.ValueOf(uint32(value.Uint())))
	case reflect.Uint64:
		return onLeaf(reflect.ValueOf(value.Uint()))
	case reflect.Float32:
		return onLeaf(reflect.ValueOf(float32(value.Float())))
	case reflect.Float64:
		return onLeaf(reflect.ValueOf(value.Float()))
	case reflect.Bool:
		return onLeaf(reflect.ValueOf(value.Bool()))
	case reflect.String:
		return onLeaf(reflect.ValueOf(value.String()))
	default:
		// Fallback
		return onLeaf(value)
	}
}
