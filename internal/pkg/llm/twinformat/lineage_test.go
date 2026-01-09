package twinformat

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizeUID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "simple", input: "orders", expected: "orders"},
		{name: "with space", input: "my orders", expected: "my_orders"},
		{name: "with dash", input: "my-orders", expected: "my_orders"},
		{name: "mixed case", input: "MyOrders", expected: "myorders"},
		{name: "complex", input: "My-Order List", expected: "my_order_list"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := sanitizeUID(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestSanitizeUID_Collisions(t *testing.T) {
	t.Parallel()

	// Document that different inputs can produce the same sanitized output.
	// This is expected behavior - the sanitizeUID function normalizes UIDs
	// for consistent lookup in the lineage graph.
	collisionGroups := [][]string{
		{"my-orders", "my_orders", "my orders"},
		{"google-ads", "google_ads", "google ads"},
		{"MyTable", "mytable", "MYTABLE"},
	}

	for _, group := range collisionGroups {
		t.Run(group[0], func(t *testing.T) {
			t.Parallel()
			expected := sanitizeUID(group[0])
			for _, input := range group[1:] {
				result := sanitizeUID(input)
				assert.Equal(t, expected, result, "inputs %q and %q should produce same output", group[0], input)
			}
		})
	}
}
