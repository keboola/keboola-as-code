package configpatch

import (
	"reflect"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Apply applies a patch structure to a config structure.
// The config structure is modified in place.
func Apply(configStruct, patchStruct any, opts ...Option) error {
	configPtr := reflect.ValueOf(configStruct)
	if configPtr.Kind() != reflect.Pointer || configPtr.IsNil() || configPtr.Elem().Kind() != reflect.Struct {
		panic(errors.Errorf(`config struct must be a pointer to a struct, found "%T"`, configStruct))
	}

	return visitConfigAndPatch(configPtr, reflect.ValueOf(patchStruct), opts, func(vc *visitContext) {
		if vc.Overwritten {
			vc.ConfigValue.Set(*vc.PatchValue)
		}
	})
}
