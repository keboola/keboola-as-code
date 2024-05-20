package definition_test

import (
	"strings"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
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

	// Test cases
	cases := testvalidation.TestCases[definition.Sink]{
		{
			Name: "empty",
			ExpectedError: `
- "projectId" is a required field
- "branchId" is a required field
- "sourceId" is a required field
- "sinkId" is a required field
- "created.at" is a required field
- "created.by" is a required field
- "version.number" is a required field
- "version.hash" is a required field
- "version.at" is a required field
- "version.by" is a required field
- "type" is a required field
- "name" is a required field
`,
			Value: definition.Sink{},
		},
		{
			Name:          "nil table section",
			ExpectedError: `"table" is a required field`,
			Value: definition.Sink{
				SinkKey:       sinkKey,
				Created:       test.Created(),
				Versioned:     test.Versioned(),
				SoftDeletable: test.SoftDeletable(),
				Type:          definition.SinkTypeTable,
				Name:          "My Source",
				Description:   "My Description",
			},
		},
		{
			Name: "empty table section",
			ExpectedError: `
- "table.type" is a required field
- "table.mapping.columns" is a required field
`,
			Value: definition.Sink{
				SinkKey:       sinkKey,
				Created:       test.Created(),
				Versioned:     test.Versioned(),
				SoftDeletable: test.SoftDeletable(),
				Type:          definition.SinkTypeTable,
				Name:          "My Source",
				Description:   "My Description",
				Table:         &definition.TableSink{},
			},
		},
		{
			Name:          "long name",
			ExpectedError: `"name" must be a maximum of 40 characters in length`,
			Value: definition.Sink{
				SinkKey:       sinkKey,
				Created:       test.Created(),
				Versioned:     test.Versioned(),
				SoftDeletable: test.SoftDeletable(),
				Type:          definition.SinkTypeTable,
				Name:          strings.Repeat("a", 40+1),
				Description:   "My Description",
				Table: &definition.TableSink{
					Type:    definition.TableTypeKeboola,
					Keboola: &definition.KeboolaTable{TableID: keboola.MustParseTableID("in.bucket.table")},
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
			Value: definition.Sink{
				SinkKey:       sinkKey,
				Created:       test.Created(),
				Versioned:     test.Versioned(),
				SoftDeletable: test.SoftDeletable(),
				Type:          definition.SinkTypeTable,
				Name:          "My Source",
				Description:   strings.Repeat("a", 4096+1),
				Table: &definition.TableSink{
					Type:    definition.TableTypeKeboola,
					Keboola: &definition.KeboolaTable{TableID: keboola.MustParseTableID("in.bucket.table")},
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
			Value: definition.Sink{
				SinkKey:       sinkKey,
				Created:       test.Created(),
				Versioned:     test.Versioned(),
				SoftDeletable: test.SoftDeletable(),
				Type:          definition.SinkTypeTable,
				Name:          "My Source",
				Description:   "My Description",
				Table: &definition.TableSink{
					Type:    definition.TableTypeKeboola,
					Keboola: &definition.KeboolaTable{TableID: keboola.MustParseTableID("in.bucket.table")},
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
			Value: definition.Sink{
				SinkKey:       sinkKey,
				Created:       test.Created(),
				Versioned:     test.Versioned(),
				SoftDeletable: test.SoftDeletable(),
				Type:          definition.SinkTypeTable,
				Name:          "My Source",
				Description:   "My Description",
				Table: &definition.TableSink{
					Type:    definition.TableTypeKeboola,
					Keboola: &definition.KeboolaTable{TableID: keboola.MustParseTableID("in.bucket.table")},
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
