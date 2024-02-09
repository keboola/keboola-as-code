package cliconfig

import (
	"encoding"
	"reflect"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func MustGenerateFlags(config any, fs *pflag.FlagSet) {
	err := GenerateFlags(config, fs)
	if err != nil {
		panic(err)
	}
}

// GenerateFlags generates flags from the config structure to the FlagSet.
// Each field tagged by "mapstructure" tag is mapped to one flag.
// The config parameter can be a structure or a pointer to a structure.
// Field can optionally have a "usage" tag.
// Field value will be set as a default value if it is not zero.
// Inspired by: https://stackoverflow.com/a/72893101
func GenerateFlags(config any, fs *pflag.FlagSet) error {
	return flagsFromStruct(config, fs, nil)
}

func flagsFromStruct(config any, fs *pflag.FlagSet, parents []string) error {
	structValue := reflect.ValueOf(config)
	if structValue.Kind() == reflect.Pointer {
		structValue = structValue.Elem()
	}

	if structValue.Kind() != reflect.Struct {
		switch {
		case !structValue.IsValid():
			return errors.Errorf(`found nil value "%s", it is not a struct or a pointer to a struct`, strings.Join(parents, "."))
		case len(parents) == 0:
			return errors.Errorf(`type "%s" is not a struct or a pointer to a struct`, structValue.Type().String())
		default:
			return errors.Errorf(`type "%s" (%s) is not a struct or a pointer to a struct`, structValue.Type().String(), strings.Join(parents, "."))
		}
	}

	structType := structValue.Type()
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		fieldValue := structValue.Field(i)

		// Ignore fields without the "mapstructure" tag
		tag, found := field.Tag.Lookup("mapstructure")
		if !found {
			continue
		}

		parts := strings.Split(tag, ",")
		partialName := parts[0]

		// Iterate a squashed/embedded struct
		if partialName == "" {
			if len(parts) == 2 && parts[1] == "squash" {
				if err := flagsFromStruct(fieldValue.Interface(), fs, parents); err != nil {
					return err
				}
			}
			continue
		}

		// Get optional usage tag
		usage := field.Tag.Get("usage")
		shorthand := field.Tag.Get("shorthand")
		fieldPath := append([]string(nil), parents...)
		fieldPath = append(fieldPath, partialName)
		flagName := strings.Join(fieldPath, ".")

		// Dereference pointer, use zero value if the value is nil
		if fieldValue.Kind() == reflect.Pointer {
			fieldType := fieldValue.Type().Elem()
			fieldValue = fieldValue.Elem()
			if !fieldValue.IsValid() {
				fieldValue = reflect.Zero(fieldType)
			}
		}

		// Detect type of the field
		switch v := fieldValue.Interface().(type) {
		case time.Duration:
			var def string
			if str := v.String(); str != "0s" {
				def = str
			}

			fs.StringP(flagName, shorthand, def, usage)
		case bool:
			fs.BoolP(flagName, shorthand, v, usage)
		case string:
			fs.StringP(flagName, shorthand, v, usage)
		case int:
			fs.IntP(flagName, shorthand, v, usage)
		case int32:
			fs.Int32P(flagName, shorthand, v, usage)
		case int64:
			fs.Int64P(flagName, shorthand, v, usage)
		case uint:
			fs.UintP(flagName, shorthand, v, usage)
		case uint32:
			fs.Uint32P(flagName, shorthand, v, usage)
		case uint64:
			fs.Uint64P(flagName, shorthand, v, usage)
		case float32:
			fs.Float32P(flagName, shorthand, v, usage)
		case float64:
			fs.Float64P(flagName, shorthand, v, usage)
		case []string:
			fs.StringSliceP(flagName, shorthand, v, usage)
		default:
			// Convert type to a pointer, some methods use pointer receiver
			if fieldValue.Kind() != reflect.Pointer {
				ptr := reflect.New(fieldValue.Type())
				ptr.Elem().Set(fieldValue)
				fieldValue = ptr
			}

			// Check if the struct implements an unmarshaler
			switch v := fieldValue.Interface().(type) {
			case encoding.TextUnmarshaler:
				var def string
				if v, ok := v.(encoding.TextMarshaler); ok && !fieldValue.IsZero() {
					if bytes, err := v.MarshalText(); err == nil {
						def = string(bytes)
					} else {
						return err
					}
				}
				fs.StringP(flagName, shorthand, def, usage)
			case encoding.BinaryUnmarshaler:
				var def string
				if v, ok := v.(encoding.BinaryMarshaler); ok && !fieldValue.IsZero() {
					if bytes, err := v.MarshalBinary(); err == nil {
						def = string(bytes)
					} else {
						return err
					}
				}
				fs.StringP(flagName, shorthand, def, usage)

			default:
				// Otherwise iterate struct fields
				parents = append([]string{}, parents...)
				if err := flagsFromStruct(fieldValue.Interface(), fs, fieldPath); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
