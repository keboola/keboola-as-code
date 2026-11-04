package twinformat

import (
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
)

func TestColumnEntriesFromDefinition(t *testing.T) {
	t.Parallel()

	integer := keboola.TypeInt
	typed := &keboola.TableDefinition{
		Columns: keboola.Columns{
			{Name: "id", BaseType: &integer, Definition: &keboola.ColumnDefinition{Type: "NUMBER", Length: "38,0", Description: "Order ID"}},
			{Name: "note", Definition: &keboola.ColumnDefinition{Type: "VARCHAR", Nullable: true}},
		},
	}
	untyped := &keboola.TableDefinition{
		Columns: keboola.Columns{
			{Name: "id", Definition: &keboola.ColumnDefinition{Description: "Order ID"}},
			{Name: "note"},
		},
	}

	cases := []struct {
		name           string
		definition     *keboola.TableDefinition
		column         string
		expectedDetail map[string]any
		expectedEntry  map[string]any
	}{
		{
			name:       "typed column with all fields",
			definition: typed,
			column:     "id",
			expectedDetail: map[string]any{
				"name": "id", "type": "NUMBER", "base_type": "INTEGER", "nullable": false, "length": "38,0", "description": "Order ID",
			},
			expectedEntry: map[string]any{"name": "id", "type": "INTEGER", "description": "Order ID"},
		},
		{
			name:           "typed column without base type, length and description",
			definition:     typed,
			column:         "note",
			expectedDetail: map[string]any{"name": "note", "type": "VARCHAR", "nullable": true},
			expectedEntry:  map[string]any{"name": "note"},
		},
		{
			name:           "untyped column has a description and no type",
			definition:     untyped,
			column:         "id",
			expectedDetail: map[string]any{"name": "id", "description": "Order ID"},
			expectedEntry:  map[string]any{"name": "id", "description": "Order ID"},
		},
		{
			name:           "untyped column without a definition",
			definition:     untyped,
			column:         "note",
			expectedDetail: map[string]any{"name": "note"},
			expectedEntry:  map[string]any{"name": "note"},
		},
		{
			name:           "column missing in the definition",
			definition:     typed,
			column:         "unknown",
			expectedDetail: map[string]any{"name": "unknown"},
			expectedEntry:  map[string]any{"name": "unknown"},
		},
		{
			name:           "typed table with nil definition",
			definition:     nil,
			column:         "id",
			expectedDetail: map[string]any{"name": "id"},
			expectedEntry:  map[string]any{"name": "id"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			columns := columnsByName(tc.definition)
			assert.Equal(t, tc.expectedDetail, buildColumnDetailEntry(tc.column, columns))
			assert.Equal(t, tc.expectedEntry, buildColumnEntry(tc.column, columns))
		})
	}
}
