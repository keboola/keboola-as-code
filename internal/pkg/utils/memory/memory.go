package memory

import (
	"reflect"
	"unsafe"
)

func Size(v any) uintptr {
	return calculateSize(reflect.ValueOf(v), make(map[uintptr]bool))
}

// calculateSize calculates the memory size of a given value, including nested or referenced elements.
func calculateSize(v reflect.Value, visited map[uintptr]bool) uintptr {
	// Handle invalid zero values immediately
	if !v.IsValid() {
		return 0
	}

	// Fundamental principle: Sizeof gives direct size, but we need to add
	// pointed-to values for reference types
	baseSize := v.Type().Size()

	switch v.Kind() {
	case reflect.Ptr:
		// Important: Prevent infinite loops with cyclic pointers
		ptrVal := v.Pointer()
		if visited[ptrVal] || ptrVal == 0 {
			return baseSize
		}
		visited[ptrVal] = true
		return baseSize + calculateSize(v.Elem(), visited)

	case reflect.Slice:
		// Slice = header + array memory
		elemSize := v.Type().Elem().Size()
		return baseSize + uintptr(v.Cap())*elemSize

	case reflect.String:
		// String header + bytes
		return baseSize + uintptr(v.Len())

	case reflect.Array:
		// Arrays have fixed size elements
		return uintptr(v.Len()) * v.Type().Elem().Size()

	case reflect.Struct:
		// Sum all fields, including unexported ones
		sum := uintptr(0)
		for i := range v.NumField() {
			sum += calculateSize(v.Field(i), visited)
		}
		return sum

	case reflect.Map, reflect.Chan, reflect.Func:
		// Warning: These types have complex internal structures
		// This is a simplified approximation
		return baseSize * 2 // Rough estimate for header+metadata

	case reflect.Interface:
		// Account for interface type word and data word
		return calculateSize(v.Elem(), visited) + unsafe.Sizeof((*any)(nil))

	default:
		// Simple types (bool, numbers) - just return base size
		return baseSize
	}
}
