package cliconfig

import (
	"reflect"
	"strings"

	"github.com/spf13/pflag"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

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
		return errors.Errorf(`type "%s" is not a struct or a pointer to a struct, it cannot be mapped to the FlagSet`, structValue.Type().String())
	}

	structType := structValue.Type()
	for i := 0; i < structType.NumField(); i++ {
		fieldType := structType.Field(i)
		fieldValue := structValue.Field(i)

		partName, found := fieldType.Tag.Lookup("mapstructure")
		if !found {
			continue
		}

		fieldPath := append(parents, partName)
		flagName := strings.Join(fieldPath, ".")
		usage := fieldType.Tag.Get("usage")

		switch fieldValue.Kind() {
		case reflect.String:
			def, _ := fieldValue.Interface().(string)
			fs.String(flagName, def, usage)
		case reflect.Int:
			def, _ := fieldValue.Interface().(int)
			fs.Int(flagName, def, usage)
		case reflect.Float64:
			def, _ := fieldValue.Interface().(float64)
			fs.Float64(flagName, def, usage)
		default:
			parents = append([]string{}, parents...)
			if err := flagsFromStruct(fieldValue.Interface(), fs, fieldPath); err != nil {
				return err
			}
		}
	}

	return nil
}
