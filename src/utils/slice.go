package utils

import (
	"fmt"
	"reflect"
	"sort"
)

// SortByName - in tests are IDs and sort random -> so we must sort by name
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
		valueI := callStringMethod(value.Index(i), "GetName") + "_" + callStringMethod(value.Index(i), "String")
		valueJ := callStringMethod(value.Index(j), "GetName") + "_" + callStringMethod(value.Index(j), "String")
		return valueI < valueJ
	})

	return slice
}

func callStringMethod(value reflect.Value, methodName string) string {
	method := value.MethodByName(methodName)
	if method.IsZero() {
		panic(fmt.Errorf("missing method \"%s\" on type \"%T\"", methodName, value))
	}
	values := method.Call(nil)
	return values[0].String()
}
