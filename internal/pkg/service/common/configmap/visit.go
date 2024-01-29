package configmap

import (
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/keboola/go-utils/pkg/orderedmap"
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

	// Try if the value implements a marshaler
	typ := vc.Type
	value := vc.Value
	if !value.IsValid() {
		// Invalid means that some parent pointer is nil, use an empty value instead
		value = reflect.New(vc.Type).Elem()
	}
	if value.Kind() != reflect.Pointer {
		// Convert type to a pointer, marshal methods may use pointers in method receivers
		ptr := reflect.New(vc.Type)
		ptr.Elem().Set(value)
		typ = ptr.Type()
		value = ptr
	}
	if !vc.Value.IsValid() || value.IsNil() {
		// The type implements a marshal methods, but the value is nil, use empty string as the value.
		switch value.Interface().(type) {
		case fmt.Stringer, json.Marshaler, encoding.TextMarshaler, encoding.BinaryMarshaler:
			return onLeaf(reflect.ValueOf(""))
		}
	} else {
		// Marshal value to a string
		switch v := value.Interface().(type) {
		case fmt.Stringer:
			return onLeaf(reflect.ValueOf(v.String()))
		case json.Marshaler:
			bytes, err := v.MarshalJSON()
			if err != nil {
				return err
			}
			return onLeaf(reflect.ValueOf(string(bytes)))
		case encoding.TextMarshaler:
			bytes, err := v.MarshalText()
			if err != nil {
				return err
			}
			return onLeaf(reflect.ValueOf(string(bytes)))
		case encoding.BinaryMarshaler:
			bytes, err := v.MarshalBinary()
			if err != nil {
				return err
			}
			return onLeaf(reflect.ValueOf(string(bytes)))
		}
	}

	// Dereference pointer, if any
	if typ.Kind() == reflect.Pointer {
		value = value.Elem()
		typ = typ.Elem()
	}

	// Try structure type
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

	// Try base types
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
