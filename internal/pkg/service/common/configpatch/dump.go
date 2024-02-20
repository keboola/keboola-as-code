package configpatch

import (
	"reflect"
	"sort"
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
	// Protected configuration key can be modified only by a super-admin.
	Protected bool `json:"protected"`
	// Validation contains validation rules of the field, if any.
	Validation string `json:"validation,omitempty"`
}

// DumpKVs generates key-value pairs from a configuration structure and a patch structure.
// Only keys found in both, configuration and patch structure, are processed.
// The structure is flattened, keys are joined with a dot "." separator.
// Each key-value pair contains information whether the value was overwritten from the patch or not.
func DumpKVs(configStruct, patchStruct any, opts ...Option) (kvs []DumpKV, err error) {
	err = visitConfigAndPatch(reflect.ValueOf(configStruct), reflect.ValueOf(patchStruct), opts, func(vc *visitContext) {
		// Generate DumpKV
		kvs = append(kvs, DumpKV{
		var value any
		if vc.Value.IsValid() {
			value = vc.Value.Interface()
		}

		var defaultValue any
		if vc.ConfigValue.IsValid() {
			defaultValue = vc.ConfigValue.Interface()
		}

			KeyPath:      vc.Config.MappedPath.String(),
			Value:        vc.Value.Interface(),
			DefaultValue: vc.ConfigValue.Interface(),
			Overwritten:  vc.Overwritten,
			Protected:    vc.Protected,
			Validation:   vc.Config.Validate,
		})
	})
	if err != nil {
		return nil, err
	}

	// Sort
	sort.SliceStable(kvs, func(i, j int) bool {
		return kvs[i].KeyPath < kvs[j].KeyPath
	})

	return kvs, nil
}
