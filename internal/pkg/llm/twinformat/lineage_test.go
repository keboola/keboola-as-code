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

func TestBuildTableUIDFromParts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bucket   string
		table    string
		expected string
	}{
		{name: "simple", bucket: "shopify", table: "orders", expected: "table:shopify/orders"},
		{name: "with dash", bucket: "google-ads", table: "campaigns", expected: "table:google_ads/campaigns"},
		{name: "complex bucket", bucket: "my bucket", table: "my table", expected: "table:my_bucket/my_table"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := BuildTableUIDFromParts(tc.bucket, tc.table)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestBuildTransformationUIDFromName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "simple", input: "process-orders", expected: "transform:process_orders"},
		{name: "with space", input: "Process Orders", expected: "transform:process_orders"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := BuildTransformationUIDFromName(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestParseTableUID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		uid            string
		expectedBucket string
		expectedTable  string
	}{
		{name: "simple", uid: "table:shopify/orders", expectedBucket: "shopify", expectedTable: "orders"},
		{name: "with underscore", uid: "table:google_ads/campaigns", expectedBucket: "google_ads", expectedTable: "campaigns"},
		{name: "no slash", uid: "table:invalid", expectedBucket: "invalid", expectedTable: ""},
		{name: "with prefix", uid: "table:bucket/table", expectedBucket: "bucket", expectedTable: "table"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bucket, table := ParseTableUID(tc.uid)
			assert.Equal(t, tc.expectedBucket, bucket)
			assert.Equal(t, tc.expectedTable, table)
		})
	}
}

func TestParseTransformationUID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		uid      string
		expected string
	}{
		{name: "simple", uid: "transform:process_orders", expected: "process_orders"},
		{name: "complex", uid: "transform:my_transformation", expected: "my_transformation"},
		{name: "no prefix", uid: "no_prefix", expected: "no_prefix"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := ParseTransformationUID(tc.uid)
			assert.Equal(t, tc.expected, result)
		})
	}
}
