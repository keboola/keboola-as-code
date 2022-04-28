package deepcopy

import (
	"fmt"
	"reflect"
)

func Copy(value interface{}) interface{} {
	return CopyTranslate(value, nil)
}

type TranslateFunc func(original, clone reflect.Value, steps Steps)

func CopyTranslate(value interface{}, callback TranslateFunc) interface{} {
	return CopyTranslateSteps(value, callback, Steps{}, make(VisitedPtrMap))
}

func CopyTranslateSteps(value interface{}, callback TranslateFunc, steps Steps, visited VisitedPtrMap) interface{} {
	if value == nil {
		return nil
	}

	// Wrap the original in a reflect.Value
	original := reflect.ValueOf(value)
	clone := reflect.New(original.Type()).Elem()
	translateRecursive(clone, original, callback, steps, visited)

	// Remove the reflection wrapper
	return clone.Interface()
}

type VisitedPtrMap map[uintptr]*reflect.Value

type CloneNestedFn func(clone reflect.Value)

// translateRecursive is modified version of https://gist.github.com/hvoecking/10772475
func translateRecursive(clone, original reflect.Value, callback TranslateFunc, steps Steps, visitedPtr VisitedPtrMap) {
	originalType := original.Type()
	cloneMethod, cloneMethodFound := originalType.MethodByName(`DeepCopy`)
	kind := original.Kind()

	// Prevent cycles
	if kind == reflect.Ptr && !original.IsNil() {
		ptr := original.Pointer()
		if v, found := visitedPtr[ptr]; found {
			clone.Set(*v)
			return
		} else {
			visitedPtr[ptr] = &clone
		}
	}

	switch {
	// Use DeepCopy method if is present, and returns right type
	case cloneMethodFound && cloneMethod.Type.Out(0).String() == originalType.String():
		values := original.MethodByName(`DeepCopy`).Call([]reflect.Value{
			reflect.ValueOf(callback),
			reflect.ValueOf(steps.Add(TypeStep{currentType: originalType.String()})),
			reflect.ValueOf(visitedPtr),
		})
		if len(values) != 2 {
			panic(fmt.Errorf(`expected two return value from %s.%s, got %d`, cloneMethod.PkgPath, cloneMethod.Name, len(values)))
		}
		clone.Set(values[0])
		if values[1].IsValid() {
			if fn, ok := values[1].Interface().(CloneNestedFn); !ok {
				panic(fmt.Errorf(`second return value from %s.%s must be "CloneNestedFn", got %d`, cloneMethod.PkgPath, cloneMethod.Name, len(values)))
			} else if fn != nil {
				fn(clone)
			}
		}
	// If it is a pointer we need to unwrap and call once again
	case kind == reflect.Ptr:
		// Check if the pointer is nil
		originalValue := original.Elem()
		if originalValue.IsValid() {
			// Allocate a new object and set the pointer to it
			clone.Set(reflect.New(originalValue.Type()))
			// Unwrap the newly created pointer
			steps := steps.Add(PointerStep{})
			translateRecursive(clone.Elem(), originalValue, callback, steps, visitedPtr)
		}

	// If it is an interface (which is very similar to a pointer), do basically the
	// same as for the pointer. Though a pointer is not the same as an interface so
	// note that we have to call Elem() after creating a new object because otherwise
	// we would end up with an actual pointer
	case kind == reflect.Interface:
		// Get rid of the wrapping interface
		originalValue := original.Elem()
		// Check if the pointer is nil
		if originalValue.IsValid() {
			// Create a new object. Now new gives us a pointer, but we want the value it
			// points to, so we have to call Elem() to unwrap it
			t := originalValue.Type()
			cloneValue := reflect.New(t).Elem()
			steps := steps.Add(InterfaceStep{targetType: t.String()})
			translateRecursive(cloneValue, originalValue, callback, steps, visitedPtr)
			clone.Set(cloneValue)
		}

	// If it is a struct we translate each field
	case kind == reflect.Struct:
		t := originalType
		for i := 0; i < original.NumField(); i += 1 {
			steps := steps.Add(StructFieldStep{currentType: originalType.String(), field: t.Field(i).Name})
			cloneField := clone.Field(i)
			if !cloneField.CanSet() {
				panic(fmt.Errorf("deepcopy found unexported field\nsteps: %s\nvalue: %#v", steps.String(), original.Interface()))
			}
			translateRecursive(cloneField, original.Field(i), callback, steps, visitedPtr)
		}

	// If it is a slice we create a new slice and translate each element
	case kind == reflect.Slice:
		if !original.IsNil() {
			clone.Set(reflect.MakeSlice(originalType, original.Len(), original.Cap()))
			for i := 0; i < original.Len(); i += 1 {
				steps := steps.Add(SliceIndexStep{index: i})
				translateRecursive(clone.Index(i), original.Index(i), callback, steps, visitedPtr)
			}
		}

	// If it is a map we create a new map and translate each value
	case kind == reflect.Map:
		if !original.IsNil() {
			clone.Set(reflect.MakeMap(originalType))
			for _, key := range original.MapKeys() {
				originalValue := original.MapIndex(key)
				// New gives us a pointer, but again we want the value
				cloneValue := reflect.New(originalValue.Type()).Elem()
				steps := steps.Add(MapKeyStep{key: key.Interface()})
				translateRecursive(cloneValue, originalValue, callback, steps, visitedPtr)
				clone.SetMapIndex(key, cloneValue)
			}
		}

	// And everything else will simply be taken from the original
	default:
		clone.Set(original)
	}

	// Custom modifications
	if callback != nil {
		callback(original, clone, steps.Add(TypeStep{currentType: kind.String()}))
	}
}
