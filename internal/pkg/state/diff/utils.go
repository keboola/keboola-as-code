package diff

import (
	"reflect"

	"github.com/google/go-cmp/cmp"
)

// OnlyOnceTransformer prevents the same transformer from running twice in a row.
// It is suitable for cases where the value is transformed only if some conditions are met, otherwise the original value is returned.
// Without this method, diff would cycle because the value type did not change.
func OnlyOnceTransformer(name string, fn interface{}) cmp.Option {
	return cmp.FilterPath(func(path cmp.Path) bool {
		if prevIndex := len(path) - 2; prevIndex >= 0 {
			if prevStep, ok := path[prevIndex].(cmp.Transform); ok && prevStep.Name() == name {
				return false
			}
		}
		return true
	}, cmp.Transformer(name, fn))
}

// CoreType unwraps all interfaces and pointers.
func CoreType(v reflect.Value) (reflect.Value, reflect.Type) {
	var t reflect.Type
	if v.IsValid() {
		t = v.Type()
	}

	for v.IsValid() && (t.Kind() == reflect.Interface || t.Kind() == reflect.Ptr) {
		if !v.IsNil() {
			v = v.Elem()
		}
		t = v.Type()
	}

	return v, t
}
