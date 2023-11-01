package definition

import (
	"strings"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
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
	switchable := Switchable{
		Enabled: true,
	}
	softDeletable := SoftDeletable{
		Deleted: false,
	}
	uploadConditions := DefaultSliceUploadConditions()
	importConditions := DefaultTableImportConditions()

	// Test cases
	cases := ValidationTestCases[Sink]{
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
				Switchable:    switchable,
				SoftDeletable: softDeletable,
				Type:          SinkTypeTable,
				Name:          "My Source",
				Description:   "My Description",
			},
		},
		{
			Name:          "nil table section",
			ExpectedError: `"table" is a required field`,
			Value: Sink{
				SinkKey:       sinkKey,
				Versioned:     versioned,
				Switchable:    switchable,
				SoftDeletable: softDeletable,
				Type:          SinkTypeTable,
				Name:          "My Source",
				Description:   "My Description",
			},
		},
		{
			Name:          "long name",
			ExpectedError: `"name" must be a maximum of 40 characters in length`,
			Value: Sink{
				SinkKey:       sinkKey,
				Versioned:     versioned,
				Switchable:    switchable,
				SoftDeletable: softDeletable,
				Type:          SinkTypeTable,
				Name:          strings.Repeat("a", 40+1),
				Description:   "My Description",
				Table: &TableSink{
					ImportConditions: importConditions,
					Mapping: TableMapping{
						TableID: keboola.MustParseTableID("in.bucket.table"),
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
				Switchable:    switchable,
				SoftDeletable: softDeletable,
				Type:          SinkTypeTable,
				Name:          "My Source",
				Description:   strings.Repeat("a", 4096+1),
				Table: &TableSink{
					ImportConditions: importConditions,
					Mapping: TableMapping{
						TableID: keboola.MustParseTableID("in.bucket.table"),
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
				Switchable:    switchable,
				SoftDeletable: softDeletable,
				Type:          SinkTypeTable,
				Name:          "My Source",
				Description:   "My Description",
				Table: &TableSink{
					ImportConditions: importConditions,
					Mapping: TableMapping{
						TableID: keboola.MustParseTableID("in.bucket.table"),
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
				Switchable:    switchable,
				SoftDeletable: softDeletable,
				Type:          SinkTypeTable,
				Name:          "My Source",
				Description:   "My Description",
				Table: &TableSink{
					UploadConditions: &uploadConditions,
					ImportConditions: importConditions,
					Mapping: TableMapping{
						TableID: keboola.MustParseTableID("in.bucket.table"),
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

func TestSliceUploadConditions_Validation(t *testing.T) {
	t.Parallel()

	// Test cases
	cases := ValidationTestCases[SliceUploadConditions]{
		{
			Name: "empty",
			ExpectedError: `
- "count" must be 1 or greater
- "size" must be 100B or greater
- "time" must be 1s or greater
`,
			Value: SliceUploadConditions{},
		},
		{
			Name: "over maximum",
			ExpectedError: `
- "count" must be 10,000,000 or less
- "size" must be 50MB or less
- "time" must be 30m0s or less
`,
			Value: SliceUploadConditions{
				Count: 10000000 + 1,
				Size:  datasize.MustParseString("50MB") + 1,
				Time:  30*time.Minute + 1,
			},
		},
		{
			Name:  "default",
			Value: DefaultSliceUploadConditions(),
		},
	}

	// Run test cases
	cases.Run(t)
}

func TestTableImportConditions_Validation(t *testing.T) {
	t.Parallel()

	// Test cases
	cases := ValidationTestCases[TableImportConditions]{
		{
			Name: "empty",
			ExpectedError: `
- "count" must be 1 or greater
- "size" must be 100B or greater
- "time" must be 30s or greater
`,
			Value: TableImportConditions{},
		},
		{
			Name: "over maximum",
			ExpectedError: `
- "count" must be 10,000,000 or less
- "size" must be 500MB or less
- "time" must be 24h0m0s or less
`,
			Value: TableImportConditions{
				Count: 10000000 + 1,
				Size:  datasize.MustParseString("500MB") + 1,
				Time:  24*time.Hour + 1,
			},
		},
		{
			Name:  "default",
			Value: DefaultTableImportConditions(),
		},
	}

	// Run test cases
	cases.Run(t)
}

func TestTableMapping_Validation(t *testing.T) {
	t.Parallel()

	// Test cases
	cases := ValidationTestCases[TableMapping]{
		{
			Name: "empty",
			ExpectedError: `
- "tableId" is a required field
- "columns" is a required field
`,
			Value: TableMapping{},
		},
		{
			Name:          "empty columns",
			ExpectedError: `"columns" must contain at least 1 item`,
			Value: TableMapping{
				TableID: keboola.MustParseTableID("in.bucket.table"),
				Columns: column.Columns{},
			},
		},
		{
			Name:          "invalid column",
			ExpectedError: `"columns[0].name" is a required field`,
			Value: TableMapping{
				TableID: keboola.MustParseTableID("in.bucket.table"),
				Columns: column.Columns{
					column.Body{},
				},
			},
		},
		{
			Name: "ok",
			Value: TableMapping{
				TableID: keboola.MustParseTableID("in.bucket.table"),
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
