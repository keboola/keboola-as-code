package utils

import (
	"fmt"
	"reflect"
	"sort"
)

// SortByName - in tests are IDs and sort random -> so we must sort by name.
func SortByName(slice interface{}) interface{} {
	// Check slice
	t := reflect.TypeOf(slice)
	if t.Kind() != reflect.Slice {
		panic(fmt.Errorf("expected slice, given \"%T\"", slice))
	}

	// Sort by Name, and by String key if names are same
	value := reflect.ValueOf(slice)
	sort.SliceStable(slice, func(i, j int) bool {
		// value = {name}_{string key}
		valueI := stringMethod(value.Index(i), "GetName", true) + "_" + stringMethod(value.Index(i), "String", false)
		valueJ := stringMethod(value.Index(j), "GetName", true) + "_" + stringMethod(value.Index(j), "String", false)
		return valueI < valueJ
	})

	return slice
}

func stringMethod(value reflect.Value, methodName string, required bool) string {
	method := value.MethodByName(methodName)
	if method.Kind() == reflect.Invalid {
		if required {
			panic(fmt.Errorf("missing method \"%s\" on type \"%s\"", methodName, value.Type().String()))
		}
		return ""
	}
	values := method.Call(nil)
	return values[0].String()
}
