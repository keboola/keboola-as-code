package configmap

import (
	"reflect"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/pflag"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func MustGenerateFlags(fs *pflag.FlagSet, v any) {
	if err := GenerateFlags(fs, v); err != nil {
		panic(err)
	}
}

// GenerateFlags generates FlagSet from the provided configuration structure.
// Each field tagged by "configKey" tag is mapped to a flag.
// Field can optionally have the "configUsage" tag.
// Field can optionally have the "configShorthand" tag.
// Inspired by: https://stackoverflow.com/a/72893101
func GenerateFlags(fs *pflag.FlagSet, v any) error {
	// Dereference pointer, if any
	value := reflect.ValueOf(v)
	if value.Kind() == reflect.Pointer {
		value = value.Elem()
	}

	// Validate type
	if value.Kind() != reflect.Struct {
		return errors.Errorf(`cannot generate flags from type "%s": it is not a struct or a pointer to a struct`, value.Type().String())
	}

	// Visit all fields and generate flags
	return Visit(value, VisitConfig{
		OnField: mapAndFilterField(),
		OnValue: func(vc *VisitContext) error {
			if !vc.Leaf {
				return nil
			}

			fieldName := vc.MappedPath.String()
			flagName := fieldToFlagName(fieldName)
			if flagName == "" {
				return nil
			}

			shorthand := vc.Shorthand
			usage := vc.Usage

			switch v := vc.PrimitiveValue.Interface().(type) {
			case int:
				fs.IntP(flagName, shorthand, v, usage)
			case int8:
				fs.Int8P(flagName, shorthand, v, usage)
			case int16:
				fs.Int16P(flagName, shorthand, v, usage)
			case int32:
				fs.Int32P(flagName, shorthand, v, usage)
			case int64:
				fs.Int64P(flagName, shorthand, v, usage)
			case uint:
				fs.UintP(flagName, shorthand, v, usage)
			case uint8:
				fs.Uint8P(flagName, shorthand, v, usage)
			case uint16:
				fs.Uint16P(flagName, shorthand, v, usage)
			case uint32:
				fs.Uint32P(flagName, shorthand, v, usage)
			case uint64:
				fs.Uint64P(flagName, shorthand, v, usage)
			case float32:
				fs.Float32P(flagName, shorthand, v, usage)
			case float64:
				fs.Float64P(flagName, shorthand, v, usage)
			case bool:
				fs.BoolP(flagName, shorthand, v, usage)
			case string:
				if !vc.Value.IsValid() || vc.Value.IsZero() {
					// Don't set the default Value, if the original Value is empty.
					// For example: empty time.Duration(0) is represented as string "0s",
					// but we don't want to show the empty value.
					v = ""
				}
				fs.StringP(flagName, shorthand, v, usage)
			case []string:
				fs.StringSliceP(flagName, shorthand, v, usage)
			case []int:
				fs.IntSliceP(flagName, shorthand, v, usage)
			case []int32:
				fs.Int32SliceP(flagName, shorthand, v, usage)
			case []int64:
				fs.Int64SliceP(flagName, shorthand, v, usage)
			case []uint:
				fs.UintSliceP(flagName, shorthand, v, usage)
			case []float32:
				fs.Float32SliceP(flagName, shorthand, v, usage)
			case []float64:
				fs.Float64SliceP(flagName, shorthand, v, usage)
			case []byte:
				fs.BytesBase64P(flagName, shorthand, v, usage)
			default:
				return errors.Errorf(`unexpected type "%T", please implement some method to convert the type to string`, vc.PrimitiveValue.Interface())
			}
			return nil
		},
	})
}

func mapAndFilterField() OnField {
	return func(field reflect.StructField, path orderedmap.Path) (fieldName string, ok bool) {
		// Field must have tag
		if tag, found := field.Tag.Lookup(configKeyTag); found {
			parts := strings.Split(tag, tagValuesSeparator)
			if fieldName = parts[0]; fieldName == "" && len(parts) == 2 && parts[1] == "squash" {
				// Iterate a squashed/embedded struct
				return "", true
			} else if fieldName != "" && fieldName != "-" {
				return fieldName, true
			}
		}

		// Otherwise skip the field
		return "", false
	}
}
