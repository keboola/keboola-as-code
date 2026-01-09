package twinformat

import (
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
)

func TestCalculateDataQuality(t *testing.T) {
	t.Parallel()

	g := &Generator{}

	tests := []struct {
		name     string
		sample   *TableSample
		expected map[string]any
	}{
		{
			name: "empty sample",
			sample: &TableSample{
				TableID:  keboola.TableID{TableName: "test"},
				Columns:  []string{},
				Rows:     [][]string{},
				RowCount: 0,
			},
			expected: map[string]any{
				"completeness":    map[string]int{},
				"null_counts":     map[string]int{},
				"distinct_counts": map[string]int{},
				"sample_size":     0,
			},
		},
		{
			name: "all non-null values",
			sample: &TableSample{
				TableID:  keboola.TableID{TableName: "test"},
				Columns:  []string{"col1", "col2"},
				Rows:     [][]string{{"a", "b"}, {"c", "d"}, {"e", "f"}},
				RowCount: 3,
			},
			expected: map[string]any{
				"completeness":    map[string]int{"col1": 100, "col2": 100},
				"null_counts":     map[string]int{"col1": 0, "col2": 0},
				"distinct_counts": map[string]int{"col1": 3, "col2": 3},
				"sample_size":     3,
			},
		},
		{
			name: "some null values",
			sample: &TableSample{
				TableID:  keboola.TableID{TableName: "test"},
				Columns:  []string{"col1", "col2"},
				Rows:     [][]string{{"a", ""}, {"", "d"}, {"e", "f"}, {"", ""}},
				RowCount: 4,
			},
			expected: map[string]any{
				"completeness":    map[string]int{"col1": 50, "col2": 50},
				"null_counts":     map[string]int{"col1": 2, "col2": 2},
				"distinct_counts": map[string]int{"col1": 2, "col2": 2},
				"sample_size":     4,
			},
		},
		{
			name: "all null values",
			sample: &TableSample{
				TableID:  keboola.TableID{TableName: "test"},
				Columns:  []string{"col1"},
				Rows:     [][]string{{""}, {""}, {""}},
				RowCount: 3,
			},
			expected: map[string]any{
				"completeness":    map[string]int{"col1": 0},
				"null_counts":     map[string]int{"col1": 3},
				"distinct_counts": map[string]int{"col1": 0},
				"sample_size":     3,
			},
		},
		{
			name: "duplicate values",
			sample: &TableSample{
				TableID:  keboola.TableID{TableName: "test"},
				Columns:  []string{"col1"},
				Rows:     [][]string{{"a"}, {"a"}, {"b"}, {"b"}, {"b"}},
				RowCount: 5,
			},
			expected: map[string]any{
				"completeness":    map[string]int{"col1": 100},
				"null_counts":     map[string]int{"col1": 0},
				"distinct_counts": map[string]int{"col1": 2},
				"sample_size":     5,
			},
		},
		{
			name: "row shorter than columns",
			sample: &TableSample{
				TableID:  keboola.TableID{TableName: "test"},
				Columns:  []string{"col1", "col2", "col3"},
				Rows:     [][]string{{"a"}, {"b", "c"}},
				RowCount: 2,
			},
			expected: map[string]any{
				"completeness":    map[string]int{"col1": 100, "col2": 50, "col3": 0},
				"null_counts":     map[string]int{"col1": 0, "col2": 1, "col3": 2},
				"distinct_counts": map[string]int{"col1": 2, "col2": 1, "col3": 0},
				"sample_size":     2,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := g.calculateDataQuality(tt.sample)
			assert.Equal(t, tt.expected, result)
		})
	}
}
