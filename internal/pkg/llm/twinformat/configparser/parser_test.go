package configparser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func TestParseStorageMappings(t *testing.T) {
	t.Parallel()

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
			result := ParseStorageMappings(tt.storage, tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseCodeBlocks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := log.NewNopLogger()

	tests := []struct {
		name     string
		params   map[string]any
		expected []*CodeBlock
	}{
		{
			name:     "empty params",
			params:   map[string]any{},
			expected: []*CodeBlock{},
		},
		{
			name: "blocks key not found",
			params: map[string]any{
				"other": "value",
			},
			expected: []*CodeBlock{},
		},
		{
			name: "blocks is not a slice",
			params: map[string]any{
				"blocks": "not a slice",
			},
			expected: []*CodeBlock{},
		},
		{
			name: "blocks is empty slice",
			params: map[string]any{
				"blocks": []any{},
			},
			expected: []*CodeBlock{},
		},
		{
			name: "block entry is not a map",
			params: map[string]any{
				"blocks": []any{"not a map"},
			},
			expected: []*CodeBlock{},
		},
		{
			name: "block with name only",
			params: map[string]any{
				"blocks": []any{
					map[string]any{
						"name": "Block 1",
					},
				},
			},
			expected: []*CodeBlock{
				{Name: "Block 1", Codes: nil},
			},
		},
		{
			name: "block with codes - script as string",
			params: map[string]any{
				"blocks": []any{
					map[string]any{
						"name": "Block 1",
						"codes": []any{
							map[string]any{
								"name":   "Code 1",
								"script": "SELECT * FROM table;",
							},
						},
					},
				},
			},
			expected: []*CodeBlock{
				{
					Name: "Block 1",
					Codes: []*Code{
						{Name: "Code 1", Script: "SELECT * FROM table;"},
					},
				},
			},
		},
		{
			name: "block with codes - script as array",
			params: map[string]any{
				"blocks": []any{
					map[string]any{
						"name": "Block 1",
						"codes": []any{
							map[string]any{
								"name":   "Code 1",
								"script": []any{"SELECT *", "FROM table;"},
							},
						},
					},
				},
			},
			expected: []*CodeBlock{
				{
					Name: "Block 1",
					Codes: []*Code{
						{Name: "Code 1", Script: "SELECT *\nFROM table;"},
					},
				},
			},
		},
		{
			name: "block with codes - scripts field (plural)",
			params: map[string]any{
				"blocks": []any{
					map[string]any{
						"name": "Block 1",
						"codes": []any{
							map[string]any{
								"name":    "Code 1",
								"scripts": []any{"line1", "line2", "line3"},
							},
						},
					},
				},
			},
			expected: []*CodeBlock{
				{
					Name: "Block 1",
					Codes: []*Code{
						{Name: "Code 1", Script: "line1\nline2\nline3"},
					},
				},
			},
		},
		{
			name: "multiple blocks with multiple codes",
			params: map[string]any{
				"blocks": []any{
					map[string]any{
						"name": "Block 1",
						"codes": []any{
							map[string]any{
								"name":   "Code 1",
								"script": "script1",
							},
							map[string]any{
								"name":   "Code 2",
								"script": "script2",
							},
						},
					},
					map[string]any{
						"name": "Block 2",
						"codes": []any{
							map[string]any{
								"name":   "Code 3",
								"script": "script3",
							},
						},
					},
				},
			},
			expected: []*CodeBlock{
				{
					Name: "Block 1",
					Codes: []*Code{
						{Name: "Code 1", Script: "script1"},
						{Name: "Code 2", Script: "script2"},
					},
				},
				{
					Name: "Block 2",
					Codes: []*Code{
						{Name: "Code 3", Script: "script3"},
					},
				},
			},
		},
		{
			name: "code with no name or script is skipped",
			params: map[string]any{
				"blocks": []any{
					map[string]any{
						"name": "Block 1",
						"codes": []any{
							map[string]any{
								"other": "field",
							},
							map[string]any{
								"name":   "Valid Code",
								"script": "valid script",
							},
						},
					},
				},
			},
			expected: []*CodeBlock{
				{
					Name: "Block 1",
					Codes: []*Code{
						{Name: "Valid Code", Script: "valid script"},
					},
				},
			},
		},
		{
			name: "block with no name or codes is skipped",
			params: map[string]any{
				"blocks": []any{
					map[string]any{
						"other": "field",
					},
					map[string]any{
						"name": "Valid Block",
					},
				},
			},
			expected: []*CodeBlock{
				{Name: "Valid Block", Codes: nil},
			},
		},
		{
			name: "queries format - SQL transformations",
			params: map[string]any{
				"queries": []any{
					"SELECT * FROM table1;",
					"SELECT * FROM table2;",
					"SELECT * FROM table3;",
				},
			},
			expected: []*CodeBlock{
				{
					Name: "queries",
					Codes: []*Code{
						{Name: "query_1", Script: "SELECT * FROM table1;"},
						{Name: "query_2", Script: "SELECT * FROM table2;"},
						{Name: "query_3", Script: "SELECT * FROM table3;"},
					},
				},
			},
		},
		{
			name: "queries with non-string entries are skipped",
			params: map[string]any{
				"queries": []any{
					"SELECT * FROM table1;",
					123,
					"SELECT * FROM table3;",
				},
			},
			expected: []*CodeBlock{
				{
					Name: "queries",
					Codes: []*Code{
						{Name: "query_1", Script: "SELECT * FROM table1;"},
						{Name: "query_3", Script: "SELECT * FROM table3;"},
					},
				},
			},
		},
		{
			name: "empty queries array",
			params: map[string]any{
				"queries": []any{},
			},
			expected: []*CodeBlock{},
		},
		{
			name: "queries is not a slice",
			params: map[string]any{
				"queries": "not a slice",
			},
			expected: []*CodeBlock{},
		},
		{
			name: "both blocks and queries",
			params: map[string]any{
				"blocks": []any{
					map[string]any{
						"name": "Block 1",
						"codes": []any{
							map[string]any{
								"name":   "Code 1",
								"script": "block script",
							},
						},
					},
				},
				"queries": []any{
					"query script",
				},
			},
			expected: []*CodeBlock{
				{
					Name: "Block 1",
					Codes: []*Code{
						{Name: "Code 1", Script: "block script"},
					},
				},
				{
					Name: "queries",
					Codes: []*Code{
						{Name: "query_1", Script: "query script"},
					},
				},
			},
		},
		{
			name: "codes is not a slice",
			params: map[string]any{
				"blocks": []any{
					map[string]any{
						"name":  "Block 1",
						"codes": "not a slice",
					},
				},
			},
			expected: []*CodeBlock{
				{Name: "Block 1", Codes: nil},
			},
		},
		{
			name: "code entry is not a map",
			params: map[string]any{
				"blocks": []any{
					map[string]any{
						"name":  "Block 1",
						"codes": []any{"not a map"},
					},
				},
			},
			expected: []*CodeBlock{
				{Name: "Block 1", Codes: nil},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ParseCodeBlocks(tt.params, false, logger, ctx)
			assert.Equal(t, tt.expected, result)
		})
	}
}
