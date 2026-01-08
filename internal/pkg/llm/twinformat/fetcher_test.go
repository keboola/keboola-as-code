package twinformat

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseStorageMappings(t *testing.T) {
	t.Parallel()

	f := &Fetcher{}

	tests := []struct {
		name     string
		storage  map[string]any
		key      string
		expected []StorageMapping
	}{
		{
			name:     "empty storage",
			storage:  map[string]any{},
			key:      "input",
			expected: []StorageMapping{},
		},
		{
			name: "key not found",
			storage: map[string]any{
				"output": map[string]any{},
			},
			key:      "input",
			expected: []StorageMapping{},
		},
		{
			name: "section is nil",
			storage: map[string]any{
				"input": nil,
			},
			key:      "input",
			expected: []StorageMapping{},
		},
		{
			name: "section has no tables key",
			storage: map[string]any{
				"input": map[string]any{
					"files": []any{},
				},
			},
			key:      "input",
			expected: []StorageMapping{},
		},
		{
			name: "tables is not a slice",
			storage: map[string]any{
				"input": map[string]any{
					"tables": "not a slice",
				},
			},
			key:      "input",
			expected: []StorageMapping{},
		},
		{
			name: "tables is empty slice",
			storage: map[string]any{
				"input": map[string]any{
					"tables": []any{},
				},
			},
			key:      "input",
			expected: []StorageMapping{},
		},
		{
			name: "table entry is not a map",
			storage: map[string]any{
				"input": map[string]any{
					"tables": []any{"not a map"},
				},
			},
			key:      "input",
			expected: []StorageMapping{},
		},
		{
			name: "table with source only",
			storage: map[string]any{
				"input": map[string]any{
					"tables": []any{
						map[string]any{
							"source": "in.c-bucket.table",
						},
					},
				},
			},
			key: "input",
			expected: []StorageMapping{
				{Source: "in.c-bucket.table", Destination: ""},
			},
		},
		{
			name: "table with destination only",
			storage: map[string]any{
				"input": map[string]any{
					"tables": []any{
						map[string]any{
							"destination": "my_table",
						},
					},
				},
			},
			key: "input",
			expected: []StorageMapping{
				{Source: "", Destination: "my_table"},
			},
		},
		{
			name: "table with both source and destination",
			storage: map[string]any{
				"input": map[string]any{
					"tables": []any{
						map[string]any{
							"source":      "in.c-bucket.table",
							"destination": "my_table",
						},
					},
				},
			},
			key: "input",
			expected: []StorageMapping{
				{Source: "in.c-bucket.table", Destination: "my_table"},
			},
		},
		{
			name: "table with neither source nor destination",
			storage: map[string]any{
				"input": map[string]any{
					"tables": []any{
						map[string]any{
							"columns": []string{"col1", "col2"},
						},
					},
				},
			},
			key:      "input",
			expected: []StorageMapping{},
		},
		{
			name: "multiple tables",
			storage: map[string]any{
				"input": map[string]any{
					"tables": []any{
						map[string]any{
							"source":      "in.c-bucket.table1",
							"destination": "table1",
						},
						map[string]any{
							"source":      "in.c-bucket.table2",
							"destination": "table2",
						},
						map[string]any{
							"source":      "in.c-bucket.table3",
							"destination": "table3",
						},
					},
				},
			},
			key: "input",
			expected: []StorageMapping{
				{Source: "in.c-bucket.table1", Destination: "table1"},
				{Source: "in.c-bucket.table2", Destination: "table2"},
				{Source: "in.c-bucket.table3", Destination: "table3"},
			},
		},
		{
			name: "output key instead of input",
			storage: map[string]any{
				"output": map[string]any{
					"tables": []any{
						map[string]any{
							"source":      "result_table",
							"destination": "out.c-bucket.result",
						},
					},
				},
			},
			key: "output",
			expected: []StorageMapping{
				{Source: "result_table", Destination: "out.c-bucket.result"},
			},
		},
		{
			name: "source and destination are not strings",
			storage: map[string]any{
				"input": map[string]any{
					"tables": []any{
						map[string]any{
							"source":      123,
							"destination": true,
						},
					},
				},
			},
			key:      "input",
			expected: []StorageMapping{},
		},
		{
			name: "mixed valid and invalid tables",
			storage: map[string]any{
				"input": map[string]any{
					"tables": []any{
						map[string]any{
							"source":      "in.c-bucket.valid",
							"destination": "valid_table",
						},
						"not a map",
						map[string]any{
							"other": "field",
						},
						map[string]any{
							"source":      "in.c-bucket.another",
							"destination": "another_table",
						},
					},
				},
			},
			key: "input",
			expected: []StorageMapping{
				{Source: "in.c-bucket.valid", Destination: "valid_table"},
				{Source: "in.c-bucket.another", Destination: "another_table"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := f.parseStorageMappings(tt.storage, tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}
