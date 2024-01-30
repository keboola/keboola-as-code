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

type visitContext struct {
	// Config is visit context of the config field.
	Config *configmap.VisitContext
	// Patch is visit context of the patch field.
	Patch *configmap.VisitContext
	// Value is PatchValue if Overwritten==true, otherwise it is ConfigValue.
	Value reflect.Value
	// PatchValue, is value of the key defined in the config structure.
	ConfigValue reflect.Value
	// PatchValue, is value of the key defined in the patch structure, can be nil.
	PatchValue *reflect.Value
	// Overwritten is true, if the value is set in the patch and is valid.
	Overwritten bool
	// Protected configuration key can be modified only by a super-admin.
	Protected bool
}

type onValue func(vc *visitContext)

// visitConfigAndPatch visits all common nested field defined in config and patch structures.
// The onValue callback is called for each found field, with actual visitContext.
func visitConfigAndPatch(configStruct, patchStruct reflect.Value, opts []Option, fn onValue) error {
	cfg := newConfig(opts)
	errs := errors.NewMultiError()

	// Visit patch
	patchKeys := make(map[string]*configmap.VisitContext)
	patchedValues := make(map[string]*reflect.Value)
	configmap.MustVisit(
		patchStruct,
		configmap.VisitConfig{
			OnField: matchTaggedFields(cfg.nameTags),
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

				// Store found key
				patchKeys[keyPath] = vc

				// Nil means not set
				if vc.Value.IsValid() && !vc.Value.IsNil() {
					v := vc.Value.Elem()
					patchedValues[keyPath] = &v
				}

				return nil
			},
		},
	)

	// Visit config
	var patchedProtected []string
	configmap.MustVisit(
		configStruct,
		configmap.VisitConfig{
			OnField: matchTaggedFields(cfg.nameTags),
			OnValue: func(configVc *configmap.VisitContext) error {
				// Process only leaf values with a field name
				if !configVc.Leaf || configVc.MappedPath.Last().String() == "" {
					return nil
				}

				keyPath := configVc.MappedPath.String()
				patchVc, ok := patchKeys[keyPath]

				// Ignore fields which are not present in the patch
				if !ok {
					return nil
				}

				// Get patched value, if any
				configValue := configVc.Value
				patchValue := patchedValues[keyPath] // can be nil
				var value reflect.Value
				if patchValue == nil {
					// The key is not overwritten by the patch, use config value
					value = configValue
				} else {
					// Deleted the map key, so unused patch keys can be processed bellow
					delete(patchedValues, keyPath)

					// Validate type
					if patchValue.Type() != configValue.Type() {
						errs.Append(errors.Errorf(
							`patch field "%s" type "%s" doesn't match config field type "%s"`,
							keyPath,
							patchValue.Type().String(),
							configValue.Type().String(),
						))
						return nil
					}

					value = *patchValue
				}

				// Note overwritten protected field
				protected := configVc.StructField.Tag.Get(cfg.protectedTag) == cfg.protectedTagValue
				if patchValue != nil && protected && !cfg.modifyProtected {
					patchedProtected = append(patchedProtected, keyPath)
				}

				// Invoke visit OnValue callback
				fn(&visitContext{
					Config:      configVc,
					Patch:       patchVc,
					Value:       value,
					ConfigValue: configValue,
					PatchValue:  patchValue,
					Overwritten: patchValue != nil,
					Protected:   protected,
				})
				return nil
			},
		},
	)

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
		return err
	}

	return nil
}

func matchTaggedFields(nameTags []string) configmap.OnField {
	return func(field reflect.StructField, path orderedmap.Path) (fieldName string, ok bool) {
		for _, nameTag := range nameTags {
			tagValue := field.Tag.Get(nameTag)
			fieldName, _, _ = strings.Cut(tagValue, ",")
			if fieldName != "" {
				break
			}
		}
		return fieldName, fieldName != ""
	}
}
