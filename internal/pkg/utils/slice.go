package utils

import (
	"fmt"
	"reflect"
	"sort"
)

type objectWithName interface {
	ObjectName() string
	String() string
}

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
		objI := value.Index(i).Interface().(objectWithName)
		objJ := value.Index(j).Interface().(objectWithName)
		// value = {name}_{string key}
		valueI := objI.ObjectName() + `_` + objI.String()
		valueJ := objJ.ObjectName() + `_` + objJ.String()
		return valueI < valueJ
	})

	return slice
}

func Contains[V comparable](slice []V, needle V) bool {
	for _, v := range slice {
		if v == needle {
			return true
		}
	}
	return false
}
