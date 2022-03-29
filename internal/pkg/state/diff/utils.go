package diff

import (
	"reflect"
)

// coreType unwraps all interfaces and pointers.
func coreType(v reflect.Value) (reflect.Value, reflect.Type) {
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
