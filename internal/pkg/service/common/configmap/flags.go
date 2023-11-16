package configmap

import (
	"reflect"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/pflag"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// StructToFlags generates FlagSet from the provided configuration structure.
// Each field tagged by "configKey" tag is mapped to a flag.
// Field can optionally have the "configUsage" tag.
// Inspired by: https://stackoverflow.com/a/72893101
func StructToFlags(fs *pflag.FlagSet, v any, outFlagToField map[string]orderedmap.Path) error {
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

			if outFlagToField != nil {
				outFlagToField[flagName] = vc.MappedPath
			}

			usage := vc.StructField.Tag.Get(configUsageTag)

			switch v := vc.PrimitiveValue.Interface().(type) {
			case int:
				fs.Int(flagName, v, usage)
			case int8:
				fs.Int8(flagName, v, usage)
			case int16:
				fs.Int16(flagName, v, usage)
			case int32:
				fs.Int32(flagName, v, usage)
			case int64:
				fs.Int64(flagName, v, usage)
			case uint:
				fs.Uint(flagName, v, usage)
			case uint8:
				fs.Uint8(flagName, v, usage)
			case uint16:
				fs.Uint16(flagName, v, usage)
			case uint32:
				fs.Uint32(flagName, v, usage)
			case uint64:
				fs.Uint64(flagName, v, usage)
			case float32:
				fs.Float32(flagName, v, usage)
			case float64:
				fs.Float64(flagName, v, usage)
			case bool:
				fs.Bool(flagName, v, usage)
			case string:
				if vc.Value.IsZero() {
					// Don't set the default Value, if the original Value is empty.
					// For example empty time.Duration(0) is represented as not empty string "0s",
					// but we don't want to show the empty string, as we do not do in other empty cases either.
					v = ""
				}
				fs.String(flagName, v, usage)
			case []string:
				fs.StringSlice(flagName, v, usage)
			case []int:
				fs.IntSlice(flagName, v, usage)
			case []int32:
				fs.Int32Slice(flagName, v, usage)
			case []int64:
				fs.Int64Slice(flagName, v, usage)
			case []uint:
				fs.UintSlice(flagName, v, usage)
			case []float32:
				fs.Float32Slice(flagName, v, usage)
			case []float64:
				fs.Float64Slice(flagName, v, usage)
			default:
				return errors.Errorf(`unexpected type "%T", please implement some method to convert the type to string`, vc.PrimitiveValue.Interface())
			}
			return nil
		},
	})
}

func mapAndFilterField() func(field reflect.StructField) (fieldName string, ok bool) {
	return func(field reflect.StructField) (fieldName string, ok bool) {
		// Field must have tag
		if tag, found := field.Tag.Lookup(configKeyTag); found {
			parts := strings.Split(tag, tagValuesSeparator)
			if fieldName = parts[0]; fieldName == "" && len(parts) == 2 && parts[1] == "squash" {
				// Iterate a squashed/embedded struct
				return "", true
			} else if fieldName != "" {
				return fieldName, true
			}
		}

		// Otherwise skip the field
		return "", false
	}
}
