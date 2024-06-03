package configpatch

import (
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// BindKVs binds flattened key-value pairs from a client request to a patch structure.
// The patch structure is modified in place.
func BindKVs(patchStruct any, kvs PatchKVs, opts ...Option) error {
	cfg := newConfig(opts)

	rootPtr := reflect.ValueOf(patchStruct)
	if rootPtr.Kind() != reflect.Pointer || rootPtr.IsNil() || rootPtr.Elem().Kind() != reflect.Struct {
		panic(errors.Errorf(`patch struct must be a pointer to a struct, found "%T"`, patchStruct))
	}

	// Convert KVs slice to a map
	patchedValues := make(map[string]reflect.Value)
	for _, kv := range kvs {
		if _, found := patchedValues[kv.KeyPath]; found {
			return errors.Errorf(`key "%s" is defined multiple times`, kv.KeyPath)
		}
		patchedValues[kv.KeyPath] = reflect.ValueOf(kv.Value)
	}

	// Visit patch, set patched values
	errs := errors.NewMultiError()
	configmap.MustVisit(
		rootPtr,
		configmap.VisitConfig{
			// InitNilPtr - all pointer are initialized before processing.
			InitNilPtr: true,
			// EmptyStructToNilPtr - pointers to empty structs are replaced by a nil value, after processing.
			EmptyStructToNilPtr: true,
			OnField:             matchTaggedFields(cfg.nameTags),
			OnValue: func(vc *configmap.VisitContext) error {
				// Process only leaf values with a field name
				if !vc.Leaf || vc.MappedPath.Last().String() == "" {
					return nil
				}

				// Patch field must be a pointer
				keyPath := vc.MappedPath.String()
				if vc.Type.Kind() != reflect.Pointer {
					errs.Append(errors.Errorf(`patch field "%s" is not a pointer, but "%s"`, keyPath, vc.Type))
					return nil
				}
				if !vc.Value.IsValid() || vc.Value.IsNil() {
					errs.Append(errors.Errorf(`patch field "%s" is not initialized`, keyPath))
					return nil
				}

				// Get patched value, if any
				value, ok := patchedValues[keyPath]
				if ok {
					// Deleted the map key, so "not found" keys can be processed bellow
					delete(patchedValues, keyPath)
				} else {
					// The key is not patched, set nil
					vc.Value.Set(reflect.Zero(vc.Type))
					return nil
				}

				// Handle string
				if str, ok := value.Interface().(string); ok {
					err := configmap.UnmarshalText([]byte(str), vc.Value)
					switch {
					case errors.As(err, &configmap.NoTextTypeError{}):
						// continue, no unmarshaler found
					case err != nil:
						// unmarshaler found, but an error occurred
						var convErr *strconv.NumError
						if errors.As(err, &convErr) {
							strShort := strhelper.Truncate(str, 20, "…")
							err = errors.Errorf(`invalid "%s" value "%s": %w`, vc.MappedPath.String(), strShort, convErr.Err)
						} else {
							err = errors.Errorf(`invalid "%s": %w`, vc.MappedPath.String(), err)
						}
						errs.Append(err)
						return nil
					default:
						// ok, unmarshalling has been successful
						return nil
					}
				}

				// Convert slice type, for example []any -> []string
				actualType := value.Type()
				expectedType := vc.Type.Elem()
				if actualType.Kind() == reflect.Slice && expectedType.Kind() == reflect.Slice && !actualType.ConvertibleTo(expectedType) {
					expectedItemType := expectedType.Elem()

					// Init empty slice
					targetSlice := vc.Value.Elem()
					targetSlice.Set(reflect.Zero(expectedType))

					// Convert items
					for index := range value.Len() {
						item := value.Index(index)
						for item.Kind() == reflect.Pointer || item.Kind() == reflect.Interface {
							item = item.Elem()
						}

						if actualItemType := item.Type(); actualItemType.ConvertibleTo(expectedItemType) {
							targetSlice.Set(reflect.Append(targetSlice, item.Convert(expectedItemType)))
						} else {
							errs.Append(errors.Errorf(`invalid "%s" value: index %d: found type "%s", expected "%s"`, keyPath, index, actualItemType, expectedItemType))
							break
						}
					}
					return nil
				}

				// Try type conversion
				if actualType.ConvertibleTo(expectedType) {
					vc.Value.Elem().Set(value.Convert(expectedType))
				} else {
					errs.Append(errors.Errorf(`invalid "%s" value: found type "%s", expected "%s"`, keyPath, actualType, expectedType))
				}

				return nil
			},
		},
	)

	// Check "not found" keys
	if len(patchedValues) > 0 {
		var notFound []string
		for keyPath := range patchedValues {
			notFound = append(notFound, keyPath)
		}
		sort.Strings(notFound)
		errs.Append(errors.Errorf(
			`key not found: "%s"`,
			strhelper.Truncate(strings.Join(notFound, `", "`), 50, "…"),
		))
	}

	return errs.ErrorOrNil()
}
