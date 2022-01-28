package deepcopy

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/spf13/cast"
)

type Steps []Step

func (s Steps) String() string {
	var out []string
	for _, item := range s {
		out = append(out, item.String())
	}
	str := strings.Join(out, `.`)
	str = strings.ReplaceAll(str, `*.`, `*`)
	str = strings.ReplaceAll(str, `.[`, `[`)
	return str
}

func (s Steps) Add(t, item string) Steps {
	newIndex := len(s)
	out := make(Steps, newIndex+1)
	copy(out, s)
	out[newIndex] = Step{Type: t, Item: item}
	return out
}

type Step struct {
	Type string
	Item string
}

func (s Step) String() string {
	if s.Item == `` {
		return s.Type
	}
	if s.Type == `` {
		return `[` + s.Item + `]`
	}
	return fmt.Sprintf(`%s[%s]`, s.Type, s.Item)
}

func Copy(value interface{}) interface{} {
	return CopyTranslate(value, nil)
}

type TranslateFunc func(original, clone reflect.Value, steps Steps)

func CopyTranslate(value interface{}, callback TranslateFunc) interface{} {
	return CopyTranslateSteps(value, callback, Steps{}, make(VisitedMap))
}

func CopyTranslateSteps(value interface{}, callback TranslateFunc, steps Steps, visited VisitedMap) interface{} {
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

type VisitedMap map[uintptr]bool

// translateRecursive is modified version of https://gist.github.com/hvoecking/10772475
func translateRecursive(clone, original reflect.Value, callback TranslateFunc, steps Steps, visited VisitedMap) {
	originalType := original.Type()
	cloneMethod, cloneMethodFound := originalType.MethodByName(`DeepCopy`)
	kind := original.Kind()

	// Prevent cycles
	if kind == reflect.Ptr && !original.IsNil() {
		ptr := original.Pointer()
		if visited[ptr] {
			panic(fmt.Errorf("deepcopy cycle detected\neach pointer can be used only once\nsteps: %s", steps.String()))
		}
		visited[ptr] = true
	}

	switch {
	// Use DeepCopy method if is present, and returns right type
	case cloneMethodFound && cloneMethod.Type.Out(0).String() == originalType.String():
		values := original.MethodByName(`DeepCopy`).Call([]reflect.Value{
			reflect.ValueOf(callback),
			reflect.ValueOf(steps.Add(originalType.String(), ``)),
			reflect.ValueOf(visited),
		})
		if len(values) != 1 {
			panic(fmt.Errorf(`expected one return value from %s.%s, got %d`, cloneMethod.PkgPath, cloneMethod.Name, len(values)))
		}
		clone.Set(values[0])
	// If it is a pointer we need to unwrap and call once again
	case kind == reflect.Ptr:
		// Check if the pointer is nil
		originalValue := original.Elem()
		if originalValue.IsValid() {
			// Allocate a new object and set the pointer to it
			clone.Set(reflect.New(originalValue.Type()))
			// Unwrap the newly created pointer
			steps := steps.Add(`*`, ``)
			translateRecursive(clone.Elem(), originalValue, callback, steps, visited)
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
			steps := steps.Add(kind.String(), t.String())
			translateRecursive(cloneValue, originalValue, callback, steps, visited)
			clone.Set(cloneValue)
		}

	// If it is a struct we translate each field
	case kind == reflect.Struct:
		t := originalType
		for i := 0; i < original.NumField(); i += 1 {
			steps := steps.Add(t.String(), t.Field(i).Name)
			cloneField := clone.Field(i)
			if !cloneField.CanSet() {
				panic(fmt.Errorf("deepcopy found unexported field\nsteps: %s\nvalue: %#v", steps.String(), original.Interface()))
			}
			translateRecursive(cloneField, original.Field(i), callback, steps, visited)
		}

	// If it is a slice we create a new slice and translate each element
	case kind == reflect.Slice:
		if !original.IsNil() {
			clone.Set(reflect.MakeSlice(originalType, original.Len(), original.Cap()))
			for i := 0; i < original.Len(); i += 1 {
				steps := steps.Add(kind.String(), strconv.Itoa(i))
				translateRecursive(clone.Index(i), original.Index(i), callback, steps, visited)
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
				steps := steps.Add(kind.String(), cast.ToString(key))
				translateRecursive(cloneValue, originalValue, callback, steps, visited)
				clone.SetMapIndex(key, cloneValue)
			}
		}

	// And everything else will simply be taken from the original
	default:
		clone.Set(original)
	}

	// Custom modifications
	if callback != nil {
		steps := steps.Add(kind.String(), ``)
		callback(original, clone, steps)
	}
}
