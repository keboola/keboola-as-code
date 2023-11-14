package configmap

import (
	"encoding"
	"encoding/json"
	"fmt"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"reflect"
)

type VisitConfig struct {
	OnField func(field reflect.StructField) (fieldName string, ok bool)
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
	Type      reflect.Type
	Sensitive bool
	Usage     string
}

func Visit(value reflect.Value, cfg VisitConfig) error {
	vc := &VisitContext{}
	vc.Value = value
	vc.PrimitiveValue = value
	vc.Type = value.Type()
	return doVisit(vc, cfg)
}

func doVisit(vc *VisitContext, cfg VisitConfig) error {
	isNil := vc.Value.Kind() == reflect.Pointer && vc.Value.IsNil()

	onPrimitiveValue := func(primitiveValue reflect.Value) error {
		vc.PrimitiveValue = primitiveValue
		return cfg.OnValue(vc)
	}

	// Dereference pointer
	originalValue := vc.Value
	for vc.Value.Kind() == reflect.Pointer && !vc.Value.IsNil() {
		vc.Value = vc.Value.Elem()
		vc.Type = vc.Type.Elem()
	}

	// Convert type to a pointer, unmarshal methods may use pointers in method receivers
	methodReceiver := originalValue
	if originalValue.Kind() != reflect.Pointer {
		ptr := reflect.New(originalValue.Type())
		ptr.Elem().Set(originalValue)
		methodReceiver = ptr
	}

	// Check if the struct implements an unmarshaler
	switch v := methodReceiver.Interface().(type) {
	case fmt.Stringer:
		if isNil {
			return onPrimitiveValue(reflect.ValueOf(""))
		}
		return onPrimitiveValue(reflect.ValueOf(v.String()))
	case json.Marshaler:
		if isNil {
			return onPrimitiveValue(reflect.ValueOf(""))
		}
		if v, err := v.MarshalJSON(); err == nil {
			return onPrimitiveValue(reflect.ValueOf(string(v)))
		} else {
			return err
		}
	case encoding.TextMarshaler:
		if isNil {
			return onPrimitiveValue(reflect.ValueOf(""))
		}
		if v, err := v.MarshalText(); err == nil {
			return onPrimitiveValue(reflect.ValueOf(string(v)))
		} else {
			return err
		}
	case encoding.BinaryMarshaler:
		if isNil {
			return onPrimitiveValue(reflect.ValueOf(""))
		}
		if bytes, err := v.MarshalBinary(); err == nil {
			return onPrimitiveValue(reflect.ValueOf(string(bytes)))
		} else {
			return err
		}
	default:
		switch vc.Value.Kind() {
		case reflect.Int:
			return onPrimitiveValue(reflect.ValueOf(int(vc.Value.Int())))
		case reflect.Int8:
			return onPrimitiveValue(reflect.ValueOf(int8(vc.Value.Int())))
		case reflect.Int16:
			return onPrimitiveValue(reflect.ValueOf(int16(vc.Value.Int())))
		case reflect.Int32:
			return onPrimitiveValue(reflect.ValueOf(int32(vc.Value.Int())))
		case reflect.Int64:
			return onPrimitiveValue(reflect.ValueOf(vc.Value.Int()))
		case reflect.Uint:
			return onPrimitiveValue(reflect.ValueOf(uint(vc.Value.Uint())))
		case reflect.Uint8:
			return onPrimitiveValue(reflect.ValueOf(uint8(vc.Value.Uint())))
		case reflect.Uint16:
			return onPrimitiveValue(reflect.ValueOf(uint16(vc.Value.Uint())))
		case reflect.Uint32:
			return onPrimitiveValue(reflect.ValueOf(uint32(vc.Value.Uint())))
		case reflect.Uint64:
			return onPrimitiveValue(reflect.ValueOf(vc.Value.Uint()))
		case reflect.Float32:
			return onPrimitiveValue(reflect.ValueOf(float32(vc.Value.Float())))
		case reflect.Float64:
			return onPrimitiveValue(reflect.ValueOf(vc.Value.Float()))
		case reflect.Bool:
			return onPrimitiveValue(reflect.ValueOf(vc.Value.Bool()))
		case reflect.String:
			return onPrimitiveValue(reflect.ValueOf(vc.Value.String()))
		case reflect.Struct:
			for i := 0; i < vc.Value.NumField(); i++ {
				// Fill context with field information
				field := &VisitContext{}
				field.StructField = vc.Type.Field(i)
				field.OriginalPath = append(vc.OriginalPath, orderedmap.MapStep(field.StructField.Name))
				field.MappedPath = vc.MappedPath
				field.Value = vc.Value.Field(i)
				field.PrimitiveValue = field.Value
				field.Type = field.Value.Type()

				// Mark field and all its children as sensitive according to the tag
				field.Sensitive = vc.Sensitive || field.StructField.Tag.Get(sensitiveTag) == "true"

				// Set usage from the tag, or use parent usage text
				field.Usage = vc.Usage
				if usage := field.StructField.Tag.Get(configUsageTag); usage != "" {
					field.Usage = usage
				}

				// Map field name, ignore skipped fields
				if fieldName, ok := cfg.OnField(field.StructField); !ok {
					continue
				} else if fieldName != "" {
					field.MappedPath = append(field.MappedPath, orderedmap.MapStep(fieldName))
				}

				// Step down
				if err := doVisit(field, cfg); err != nil {
					return err
				}
			}
		default:
			return onPrimitiveValue(vc.Value)
		}
	}

	return nil
}
