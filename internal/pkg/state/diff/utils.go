package diff

import (
	"reflect"

	"github.com/google/go-cmp/cmp"
)

// OnlyOnceTransformer prevents the same transformer from running twice in a row.
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
		if t.Kind() == reflect.Interface || t.Kind() == reflect.Ptr {
			if !v.IsZero() {
				v = v.Elem()
			}
			t = v.Type()
		}
	}
	return v, t
}
