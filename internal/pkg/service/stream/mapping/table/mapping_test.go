package table_test

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testvalidation"
)

func TestTableMapping_Validation(t *testing.T) {
	t.Parallel()

	// Test cases
	cases := testvalidation.TestCases[table.Mapping]{
		{
			Name:          "empty",
			ExpectedError: `"columns" is a required field`,
			Value:         table.Mapping{},
		},
		{
			Name:          "empty columns",
			ExpectedError: `"columns" must contain at least 1 item`,
			Value: table.Mapping{
				Columns: column.Columns{},
			},
		},
		{
			Name:          "invalid column",
			ExpectedError: `"columns[0].name" is a required field`,
			Value: table.Mapping{
				Columns: column.Columns{
					column.Body{},
				},
			},
		},
		{
			Name: "ok",
			Value: table.Mapping{
				Columns: column.Columns{
					column.Body{
						Name: "body",
					},
				},
			},
		},
	}

	// Run test cases
	cases.Run(t)
}
