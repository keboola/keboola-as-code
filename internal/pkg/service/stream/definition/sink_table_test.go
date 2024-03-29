package definition

import (
	"strings"
	"testing"
	"time"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testvalidation"
)

func TestTableSink_Validation(t *testing.T) {
	t.Parallel()

	sinkKey := key.SinkKey{
		SourceKey: key.SourceKey{
			BranchKey: key.BranchKey{
				ProjectID: 123,
				BranchID:  456,
			},
			SourceID: "my-source",
		},
		SinkID: "my-sink",
	}
	versioned := Versioned{
		Version: Version{
			Number:      1,
			Hash:        "0123456789123456",
			ModifiedAt:  utctime.From(time.Now()),
			Description: "foo bar",
		},
	}
	softDeletable := SoftDeletable{
		Deleted: false,
	}

	// Test cases
	cases := testvalidation.TestCases[Sink]{
		{
			Name: "empty",
			ExpectedError: `
- "projectId" is a required field
- "branchId" is a required field
- "sourceId" is a required field
- "sinkId" is a required field
- "version.number" is a required field
- "version.hash" is a required field
- "version.modifiedAt" is a required field
- "type" is a required field
- "name" is a required field
`,
			Value: Sink{},
		},
		{
			Name:          "nil table section",
			ExpectedError: `"table" is a required field`,
			Value: Sink{
				SinkKey:       sinkKey,
				Versioned:     versioned,
				SoftDeletable: softDeletable,
				Type:          SinkTypeTable,
				Name:          "My Source",
				Description:   "My Description",
			},
		},
		{
			Name: "empty table section",
			ExpectedError: `
- "table.keboola.tableId" is a required field
- "table.mapping.columns" is a required field
`,
			Value: Sink{
				SinkKey:       sinkKey,
				Versioned:     versioned,
				SoftDeletable: softDeletable,
				Type:          SinkTypeTable,
				Name:          "My Source",
				Description:   "My Description",
				Table:         &TableSink{},
			},
		},
		{
			Name:          "long name",
			ExpectedError: `"name" must be a maximum of 40 characters in length`,
			Value: Sink{
				SinkKey:       sinkKey,
				Versioned:     versioned,
				SoftDeletable: softDeletable,
				Type:          SinkTypeTable,
				Name:          strings.Repeat("a", 40+1),
				Description:   "My Description",
				Table: &TableSink{
					Keboola: TableSinkKeboola{TableID: keboola.MustParseTableID("in.bucket.table")},
					Mapping: table.Mapping{
						Columns: column.Columns{
							column.Body{
								Name: "body",
							},
						},
					},
				},
			},
		},
		{
			Name:          "long description",
			ExpectedError: `"description" must be a maximum of 4,096 characters in length`,
			Value: Sink{
				SinkKey:       sinkKey,
				Versioned:     versioned,
				SoftDeletable: softDeletable,
				Type:          SinkTypeTable,
				Name:          "My Source",
				Description:   strings.Repeat("a", 4096+1),
				Table: &TableSink{
					Keboola: TableSinkKeboola{TableID: keboola.MustParseTableID("in.bucket.table")},
					Mapping: table.Mapping{
						Columns: column.Columns{
							column.Body{
								Name: "body",
							},
						},
					},
				},
			},
		},
		{
			Name: "minimal",
			Value: Sink{
				SinkKey:       sinkKey,
				Versioned:     versioned,
				SoftDeletable: softDeletable,
				Type:          SinkTypeTable,
				Name:          "My Source",
				Description:   "My Description",
				Table: &TableSink{
					Keboola: TableSinkKeboola{TableID: keboola.MustParseTableID("in.bucket.table")},
					Mapping: table.Mapping{
						Columns: column.Columns{
							column.Body{
								Name: "body",
							},
						},
					},
				},
			},
		},
		{
			Name: "with custom upload conditions",
			Value: Sink{
				SinkKey:       sinkKey,
				Versioned:     versioned,
				SoftDeletable: softDeletable,
				Type:          SinkTypeTable,
				Name:          "My Source",
				Description:   "My Description",
				Table: &TableSink{
					Keboola: TableSinkKeboola{TableID: keboola.MustParseTableID("in.bucket.table")},
					Mapping: table.Mapping{
						Columns: column.Columns{
							column.Body{
								Name: "body",
							},
						},
					},
				},
			},
		},
	}

	// Run test cases
	cases.Run(t)
}

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
