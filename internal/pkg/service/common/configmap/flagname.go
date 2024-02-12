package configmap

import (
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"reflect"
	"strings"

	"github.com/umisama/go-regexpcache"
)

// flagFieldMapTo visits the structs in depth and generates a map: flagName => fieldPath.
func newFlagToFieldMap(structs ...any) (map[string]orderedmap.Path, error) {
	out := make(map[string]orderedmap.Path)
	for _, s := range structs {
		if err := flagFieldMapTo(s, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// flagFieldMapTo visits the struct in depth and generates a map: flagName => fieldPath.
func flagFieldMapTo(s any, out map[string]orderedmap.Path) error {
	// Dereference pointer, if any
	value := reflect.ValueOf(s)
	if value.Kind() == reflect.Pointer {
		value = value.Elem()
	}

	// Validate type
	if value.Kind() != reflect.Struct {
		return errors.Errorf(`cannot generate flags from type "%s": it is not a struct or a pointer to a struct`, value.Type().String())
	}

	// Visit all fields and generate flag names
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

			if _, ok := out[flagName]; ok {
				return errors.Errorf(`flag "%s" is defined multiple times`, flagName)
			}

			out[flagName] = vc.MappedPath

			return nil
		},
	})
}

func fieldToFlagName(fieldName string) string {
	str := regexpcache.MustCompile(`[A-Z]+`).ReplaceAllString(fieldName, "-$0")
	str = regexpcache.MustCompile(`[-.\s]+`).ReplaceAllString(str, "-")
	str = strings.Trim(str, "-")
	str = strings.ToLower(str)
	return str
}
