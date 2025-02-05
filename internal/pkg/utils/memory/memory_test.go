package memory

import (
	"reflect"
	"testing"
	"unsafe"
)

func TestCalculateSize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    interface{}
		expected uintptr
	}{
		{name: "Nil value", input: nil, expected: 0},
		{name: "Empty string", input: "", expected: unsafe.Sizeof("")},
		{name: "Non-empty string", input: "hello", expected: unsafe.Sizeof("") + uintptr(len("hello"))},
		{name: "Int value", input: 42, expected: unsafe.Sizeof(42)},
		{name: "Pointer to int", input: func() interface{} { i := 42; return &i }(), expected: unsafe.Sizeof(0) + unsafe.Sizeof(42)},
		{name: "Slice with capacity", input: make([]int, 0, 10), expected: unsafe.Sizeof([]int{}) + uintptr(10)*unsafe.Sizeof(0)},
		{name: "Struct with fields", input: struct {
			a int8
			b int16
			c int32
		}{}, expected: unsafe.Sizeof(int8(0)) + unsafe.Sizeof(int16(0)) + unsafe.Sizeof(int32(0))},
		{name: "Array of int", input: [3]int{1, 2, 3}, expected: uintptr(3) * unsafe.Sizeof(0)},
		{name: "Map value", input: map[string]int{}, expected: 2 * unsafe.Sizeof(map[string]int{})},
		{name: "Channel value", input: make(chan int), expected: 2 * unsafe.Sizeof(make(chan int))},
		{name: "Function value", input: (func())(nil), expected: 2 * unsafe.Sizeof((func())(nil))},
		{name: "Interface value", input: (interface{})(42), expected: unsafe.Sizeof((*interface{})(nil))},
	}

	for _, tc := range tests {
		t.Parallel()
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			visited := make(map[uintptr]bool)
			v := reflect.ValueOf(tc.input)
			got := calculateSize(v, visited)
			if got != tc.expected {
				t.Errorf("calculateSize(%v) = %d, want %d", tc.input, got, tc.expected)
			}
		})
	}
}
