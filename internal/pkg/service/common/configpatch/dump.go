package configpatch

import (
	"reflect"
	"sort"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// DumpKV is result of the patch operation for a one configuration key.
type DumpKV struct {
	// KeyPath is a configuration key, parts are joined with a dot "." separator.
	KeyPath string `json:"key"`
	// Value is an actual value of the configuration key.
	Value any `json:"value"`
	// DefaultValue of the configuration key.
	DefaultValue any `json:"defaultValue"`
	// Overwritten is true, if the DefaultValue was replaced by a value from the path.
	Overwritten bool `json:"overwritten"`
	// Protected configuration keys can be modified only by a super-admin.
	Protected bool `json:"protected"`
	// Validation contains validation rules of the field, if any.
	Validation string `json:"validation,omitempty"`
}

// DumpKVs generates key-value pairs from a configuration structure and a patch structure.
// Only keys found in both, configuration and patch structure, are processed.
// The structure is flattened, keys are joined with a dot "." separator.
// Each key-value pair contains information whether the value was overwritten from the patch or not.
func DumpKVs(configStruct, patchStruct any, opts ...Option) ([]DumpKV, error) {
	cfg := newConfig(opts)

	var kvs []DumpKV
	errs := errors.NewMultiError()

	// Visit patch, get patched values
	patchKeys := make(map[string]bool)
	patchedValues := make(map[string]reflect.Value)
	visitStruct(cfg.nameTags, patchStruct, func(vc *configmap.VisitContext) {
		// Process only leaf values with a field name
		if !vc.Leaf || vc.MappedPath.Last().String() == "" {
			return
		}

		// Patch field must be a pointer
		keyPath := vc.MappedPath.String()
		if vc.Type.Kind() != reflect.Pointer {
			errs.Append(errors.Errorf(`patch field "%s" is not a pointer, but "%s"`, keyPath, vc.Type))
			return
		}

		// Store found key
		patchKeys[keyPath] = true

		// Nil means not set
		if vc.Value.IsValid() && !vc.Value.IsNil() {
			patchedValues[keyPath] = vc.Value.Elem()
		}
	})

	// Visit config, generate output key-value pairs
	var patchedProtected []string
	visitStruct(cfg.nameTags, configStruct, func(vc *configmap.VisitContext) {
		// Process only leaf values with a field name
		if !vc.Leaf || vc.MappedPath.Last().String() == "" {
			return
		}

		// Ignore fields which are not present in the patch
		keyPath := vc.MappedPath.String()
		if !patchKeys[keyPath] {
			return
		}

		// Get patched value, if any
		defaultValue := vc.Value
		value, overwritten := patchedValues[keyPath]
		if overwritten {
			// Deleted the map key, so unused patch keys can be processed bellow
			delete(patchedValues, keyPath)

			// Validate type
			if value.Type() != defaultValue.Type() {
				errs.Append(errors.Errorf(
					`patch field "%s" type "%s" doesn't match config field type "%s"`,
					keyPath,
					value.Type().String(),
					defaultValue.Type().String(),
				))
				return
			}
		} else {
			// The key is not overwritten by the patch, use default value
			value = defaultValue
		}

		// Note overwritten protected field
		protected := vc.StructField.Tag.Get(cfg.protectedTag) == cfg.protectedTagValue
		if overwritten && protected && !cfg.modifyProtected {
			patchedProtected = append(patchedProtected, keyPath)
		}

		// Generate DumpKV
		kvs = append(kvs, DumpKV{
			KeyPath:      keyPath,
			Value:        value.Interface(),
			DefaultValue: defaultValue.Interface(),
			Overwritten:  overwritten,
			Protected:    protected,
			Validation:   vc.Validate,
		})
	})

	// Check unexpected patch keys
	if len(patchedValues) > 0 {
		var unused []string
		for keyPath := range patchedValues {
			unused = append(unused, keyPath)
		}
		sort.Strings(unused)
		errs.Append(errors.Errorf(
			`patch contains unexpected keys: "%s"`,
			strhelper.Truncate(strings.Join(unused, `", "`), 50, "…"),
		))
	}

	// Check overwritten protected keys
	if len(patchedProtected) > 0 {
		errs.Append(errors.Errorf(`cannot modify protected fields: "%s"`, strings.Join(patchedProtected, `", "`)))
	}

	// Check errors
	if err := errs.ErrorOrNil(); err != nil {
		return nil, err
	}

	// Sort
	sort.SliceStable(kvs, func(i, j int) bool {
		return kvs[i].KeyPath < kvs[j].KeyPath
	})

	return kvs, nil
}

func visitStruct(nameTags []string, root any, onValue func(vc *configmap.VisitContext)) {
	err := configmap.Visit(
		reflect.ValueOf(root),
		configmap.VisitConfig{
			OnField: func(field reflect.StructField, path orderedmap.Path) (fieldName string, ok bool) {
				for _, nameTag := range nameTags {
					tagValue := field.Tag.Get(nameTag)
					fieldName, _, _ = strings.Cut(tagValue, ",")
					if fieldName != "" {
						break
					}
				}
				return fieldName, fieldName != ""
			},
			OnValue: func(vc *configmap.VisitContext) error {
				onValue(vc)
				return nil
			},
		},
	)
	// OnValue function returns no error, so no error is expected at all
	if err != nil {
		panic(errors.New("no error expected"))
	}
}
