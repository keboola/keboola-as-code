package configpatch

import (
	"reflect"
	"sort"
)

// DumpAll generates key-value pairs from a configuration structure and a patch structure.
// Only keys found in both, configuration and patch structure, are dumped.
// The structure is flattened, keys are joined with a dot "." separator.
// Each key-value pair contains information whether the value was overwritten from the patch or not.
func DumpAll(configStruct, patchStruct any, opts ...Option) (kvs []ConfigKV, err error) {
	err = visitConfigAndPatch(reflect.ValueOf(configStruct), reflect.ValueOf(patchStruct), opts, func(vc *visitContext) {
		var value any
		if vc.Value.IsValid() {
			value = vc.Value.Interface()
		}

		var defaultValue any
		if vc.ConfigValue.IsValid() {
			defaultValue = vc.ConfigValue.Interface()
		}

		kvs = append(kvs, ConfigKV{
			KeyPath:      vc.Config.MappedPath.String(),
			Value:        value,
			DefaultValue: defaultValue,
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

func MustDumpPatch(configStruct, patchStruct any, opts ...Option) PatchKVs {
	kvs, err := DumpPatch(configStruct, patchStruct, opts...)
	if err != nil {
		panic(err)
	}
	return kvs
}

// DumpPatch generates key-value pairs from a configuration structure and a patch structure.
// Only patched keys are dumped.
// The structure is flattened, keys are joined with a dot "." separator.
// Each key-value pair contains information whether the value was overwritten from the patch or not.
func DumpPatch(configStruct, patchStruct any, opts ...Option) (kvs PatchKVs, err error) {
	err = visitConfigAndPatch(reflect.ValueOf(configStruct), reflect.ValueOf(patchStruct), opts, func(vc *visitContext) {
		if !vc.Overwritten {
			return
		}

		var value any
		if vc.Value.IsValid() {
			value = vc.Value.Interface()
		}

		// Generate ConfigKV
		kvs = append(kvs, PatchKV{
			KeyPath: vc.Config.MappedPath.String(),
			Value:   value,
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
