package configmap

import (
	"reflect"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type OnField func(field reflect.StructField, path orderedmap.Path) (fieldName string, ok bool)

type OnValue func(vc *VisitContext) error

type VisitConfig struct {
	// InitNilPtr initializes each found nil pointer with an empty struct.
	// The operation is performed before all nested field are processed.
	InitNilPtr bool
	// EmptyStructToNilPtr replaces each found empty struct with nil pointer, if possible.
	// The operation is performed after all nested field are processed.
	EmptyStructToNilPtr bool
	// OnField maps field to a custom field name, for example from a tag.
	// If ok == false, then the field is ignored.
	OnField OnField
	// OnValue is called on each field
	OnValue OnValue
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
	// Usage contains value from the "configShorthand" tag, if any.
	Shorthand string
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

// MustVisit is similar to the Visit method, but no error is expected.
func MustVisit(value reflect.Value, cfg VisitConfig) {
	if err := Visit(value, cfg); err != nil {
		panic(errors.New("no error expected"))
	}
}

func doVisit(vc *VisitContext, cfg VisitConfig) error {
	onLeaf := func(primitiveValue reflect.Value) error {
		vc.PrimitiveValue = primitiveValue
		vc.Leaf = true
		return cfg.OnValue(vc)
	}

	// Handle text
	text, err := MarshalText(vc.Type, vc.Value)
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
		return doVisitStruct(vc, cfg, typ, value)
	}

	// Handle base types
	switch value.Kind() {
	case reflect.Int:
		return onLeaf(reflect.ValueOf(int(value.Int())))
	case reflect.Int8:
		i, err := safecast.Convert[int8](value.Int())
		if err != nil {
			return err
		}
		return onLeaf(reflect.ValueOf(i))
	case reflect.Int16:
		i, err := safecast.Convert[int16](value.Int())
		if err != nil {
			return err
		}
		return onLeaf(reflect.ValueOf(i))
	case reflect.Int32:
		i, err := safecast.Convert[int32](value.Int())
		if err != nil {
			return err
		}
		return onLeaf(reflect.ValueOf(i))
	case reflect.Int64:
		return onLeaf(reflect.ValueOf(value.Int()))
	case reflect.Uint:
		return onLeaf(reflect.ValueOf(uint(value.Uint())))
	case reflect.Uint8:
		i, err := safecast.Convert[uint8](value.Uint())
		if err != nil {
			return err
		}
		return onLeaf(reflect.ValueOf(i))
	case reflect.Uint16:
		i, err := safecast.Convert[uint16](value.Uint())
		if err != nil {
			return err
		}
		return onLeaf(reflect.ValueOf(i))
	case reflect.Uint32:
		i, err := safecast.Convert[uint32](value.Uint())
		if err != nil {
			return err
		}
		return onLeaf(reflect.ValueOf(i))
	case reflect.Uint64:
		i, err := safecast.Convert[uint64](value.Uint())
		if err != nil {
			return err
		}
		return onLeaf(reflect.ValueOf(i))
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

func doVisitStruct(vc *VisitContext, cfg VisitConfig, typ reflect.Type, value reflect.Value) error {
	// Call callback
	if err := cfg.OnValue(vc); err != nil {
		return err
	}

	for i := range typ.NumField() {
		// Fill context with field information
		field := &VisitContext{}
		field.StructField = typ.Field(i)
		field.OriginalPath = append(field.OriginalPath, vc.OriginalPath...)
		field.OriginalPath = append(field.OriginalPath, orderedmap.MapStep(field.StructField.Name))
		field.MappedPath = append(field.MappedPath, vc.MappedPath...)
		field.Type = field.StructField.Type
		field.Leaf = false

		// Get field value, if the parent struct is valid/defined.
		if value.IsValid() {
			// Get field value
			fv := value.Field(i)
			field.Value = fv
			field.PrimitiveValue = fv

			// Initialize nil pointer with an empty struct. It is used by the configpatch.BindKVs.
			if cfg.InitNilPtr && fv.Kind() == reflect.Pointer && fv.IsNil() {
				fv.Set(reflect.New(field.Type.Elem()))
			}
		}

		// Mark field and all its children as sensitive according to the tag
		field.Sensitive = vc.Sensitive || field.StructField.Tag.Get(sensitiveTag) == "true"

		// Set usage from the tag, or use parent value
		field.Usage = vc.Usage
		if usage := field.StructField.Tag.Get(configUsageTag); usage != "" {
			field.Usage = usage
		}

		// Set shorthand from the tag, or use parent value
		field.Shorthand = vc.Shorthand
		if shorthand := field.StructField.Tag.Get(configShorthandTag); shorthand != "" {
			field.Shorthand = shorthand
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

	// Set nil, if the value is a pointer to an empty struct
	if cfg.EmptyStructToNilPtr {
		if vc.Value.CanAddr() && vc.Value.Kind() == reflect.Pointer && vc.Value.Elem().Kind() == reflect.Struct && vc.Value.Elem().IsZero() {
			vc.Value.Set(reflect.Zero(vc.Value.Type())) // nil is zero value for a pointer
		}
	}

	return nil
}
