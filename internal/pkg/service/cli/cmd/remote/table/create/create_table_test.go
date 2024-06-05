package create

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/relvacode/iso8601"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/interactive"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/table"
)

func ColumnsInput() string {
	return `[{"name": "id","definition": {"type": "INT"},"basetype": "NUMERIC"},{"name": "name","definition": {"type": "STRING"},"basetype": "STRING"}]`
}

// BigQuery options.
func OptionsInput() string {
	return `{ "timePartitioning": {
        "type": "DAY",
        "expirationMs": "864000000",
        "field": "time"
    },
    "clustering": {
        "fields": [
            "id"
        ]
    },
    "rangePartitioning": {
        "field": "id",
        "range": {
            "start": "0",
            "end": "10",
            "interval": "1"
        }
    }}`
}

func TestGetCreateRequest(t *testing.T) {
	t.Parallel()
	type args struct {
		columns []string
	}
	tests := []struct {
		name string
		args args
		want []keboola.Column
	}{
		{
			name: "getCreateTableRequest",
			args: args{columns: []string{"id", "name"}},
			want: []keboola.Column{
				{
					Name: "id",
				},
				{
					Name: "name",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equalf(t, tt.want, getOptionCreateRequest(tt.args.columns), "getOptionCreateRequest(%v)", tt.args.columns)
		})
	}
}

func TestAskCreate(t *testing.T) {
	t.Parallel()

	args := []string{}

	var buckets []*keboola.Bucket

	branch, bucket := getBranchAndBucket()

	buckets = append(buckets, bucket)

	t.Run("columns-from interactive", func(t *testing.T) {
		t.Parallel()

		d, console := dialog.NewForTest(t, true)

		deps := dependencies.NewMocked(t, dependencies.WithSnowflakeBackend())
		// Set fake file editor
		d.Prompt.(*interactive.Prompt).SetEditor(`true`)
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()

			assert.NoError(t, console.ExpectString("Select a bucket:"))

			assert.NoError(t, console.Send("in.c-test_1214124"))

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Enter the table name."))

			assert.NoError(t, console.ExpectString("Table name:"))

			assert.NoError(t, console.Send("table1"))

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Want to define column types?"))

			assert.NoError(t, console.Send("Y"))

			assert.NoError(t, console.SendEnter()) // confirm

			assert.NoError(t, console.ExpectString("Columns definitions"))

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Select columns for primary key"))

			assert.NoError(t, console.ExpectString("id"))

			assert.NoError(t, console.ExpectString("name"))

			assert.NoError(t, console.SendSpace())

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Select columns for primary key: id"))
		}()

		res, err := AskCreateTable(args, branch.BranchKey, buckets, d, Flags{}, deps.ProjectBackends())
		assert.NoError(t, err)
		wg.Wait()

		assert.Equal(t, table.Options{
			CreateTableRequest: keboola.CreateTableRequest{
				TableDefinition: keboola.TableDefinition{
					PrimaryKeyNames: []string{"id"},
					Columns: []keboola.Column{
						{
							Name: "id",
							Definition: &keboola.ColumnDefinition{
								Type:     "VARCHAR",
								Nullable: false,
							},
							BaseType: ptr.Ptr(keboola.TypeString),
						},
						{
							Name: "name",
							Definition: &keboola.ColumnDefinition{
								Type:     "VARCHAR",
								Nullable: false,
							},
							BaseType: ptr.Ptr(keboola.TypeString),
						},
					},
				},
				Name: "table1",
			},
			BucketKey: keboola.BucketKey{
				BranchID: branch.ID,
				BucketID: bucket.BucketID,
			},
		}, res)
	})

	t.Run("columns name interactive", func(t *testing.T) {
		t.Parallel()

		d, console := dialog.NewForTest(t, true)
		deps := dependencies.NewMocked(t, dependencies.WithSnowflakeBackend())
		// Set fake file editor
		d.Prompt.(*interactive.Prompt).SetEditor(`true`)

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()

			assert.NoError(t, console.ExpectString("Select a bucket:"))

			assert.NoError(t, console.Send("in.c-test_1214124"))

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Enter the table name."))

			assert.NoError(t, console.ExpectString("Table name:"))

			assert.NoError(t, console.Send("table1"))

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Want to define column types?"))

			assert.NoError(t, console.Send("N"))

			assert.NoError(t, console.SendEnter()) // confirm

			assert.NoError(t, console.ExpectString("Enter a comma-separated list of column names."))

			assert.NoError(t, console.Send("id,name"))

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Select columns for primary key"))

			assert.NoError(t, console.ExpectString("id"))

			assert.NoError(t, console.ExpectString("name"))

			assert.NoError(t, console.SendSpace())

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Select columns for primary key: id"))
		}()

		res, err := AskCreateTable(args, branch.BranchKey, buckets, d, Flags{}, deps.ProjectBackends())
		assert.NoError(t, err)
		wg.Wait()

		assert.Equal(t, table.Options{
			CreateTableRequest: keboola.CreateTableRequest{
				TableDefinition: keboola.TableDefinition{
					PrimaryKeyNames: []string{"id"},
					Columns: []keboola.Column{
						{
							Name: "id",
						},
						{
							Name: "name",
						},
					},
				},
				Name: "table1",
			},
			BucketKey: keboola.BucketKey{
				BranchID: branch.ID,
				BucketID: bucket.BucketID,
			},
		}, res)
	})

	t.Run("columns-from flag", func(t *testing.T) {
		t.Parallel()

		d, console := dialog.NewForTest(t, true)
		deps := dependencies.NewMocked(t, dependencies.WithSnowflakeBackend())
		// Set fake file editor
		d.Prompt.(*interactive.Prompt).SetEditor(`true`)

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()

			assert.NoError(t, console.ExpectString("Select a bucket:"))

			assert.NoError(t, console.Send("in.c-test_1214124"))

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Enter the table name."))

			assert.NoError(t, console.ExpectString("Table name:"))

			assert.NoError(t, console.Send("table1"))

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Select columns for primary key"))

			assert.NoError(t, console.ExpectString("id"))

			assert.NoError(t, console.ExpectString("name"))

			assert.NoError(t, console.SendSpace())

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Select columns for primary key: id"))
		}()

		tempDir := t.TempDir()

		// Create a temporary file within the temporary directory
		tempFile, err := os.Create(filepath.Join(tempDir, "foo.json")) // nolint:forbidigo
		require.NoError(t, err)

		defer tempFile.Close()

		// Write content to the temporary file
		_, err = tempFile.Write([]byte(ColumnsInput()))
		require.NoError(t, err)

		// Get the file path of the temporary file
		filePath := tempFile.Name()

		// set flag columns-from
		f := Flags{
			ColumnsFrom: configmap.NewValueWithOrigin(filePath, configmap.SetByFlag),
		}
		res, err := AskCreateTable(args, branch.BranchKey, buckets, d, f, deps.ProjectBackends())
		assert.NoError(t, err)
		wg.Wait()

		assert.Equal(t, table.Options{
			CreateTableRequest: keboola.CreateTableRequest{
				TableDefinition: keboola.TableDefinition{
					PrimaryKeyNames: []string{"id"},
					Columns: []keboola.Column{
						{
							Name: "id",
							Definition: &keboola.ColumnDefinition{
								Type:     "INT",
								Nullable: false,
							},
							BaseType: ptr.Ptr(keboola.TypeNumeric),
						},
						{
							Name: "name",
							Definition: &keboola.ColumnDefinition{
								Type:     "STRING",
								Nullable: false,
							},
							BaseType: ptr.Ptr(keboola.TypeString),
						},
					},
				},
				Name: "table1",
			},
			BucketKey: keboola.BucketKey{
				BranchID: branch.ID,
				BucketID: bucket.BucketID,
			},
		}, res)
	})

	t.Run("columns name from flag", func(t *testing.T) {
		t.Parallel()

		d, console := dialog.NewForTest(t, true)
		deps := dependencies.NewMocked(t, dependencies.WithSnowflakeBackend())
		// Set fake file editor
		d.Prompt.(*interactive.Prompt).SetEditor(`true`)

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()

			assert.NoError(t, console.ExpectString("Select a bucket:"))

			assert.NoError(t, console.Send("in.c-test_1214124"))

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Enter the table name."))

			assert.NoError(t, console.ExpectString("Table name:"))

			assert.NoError(t, console.Send("table1"))

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Select columns for primary key"))

			assert.NoError(t, console.ExpectString("id"))

			assert.NoError(t, console.ExpectString("name"))

			assert.NoError(t, console.SendSpace())

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Select columns for primary key: id"))
		}()

		// set flag --columns
		f := Flags{
			Columns: configmap.NewValueWithOrigin([]string{"id", "name"}, configmap.SetByFlag),
		}
		res, err := AskCreateTable(args, branch.BranchKey, buckets, d, f, deps.ProjectBackends())
		assert.NoError(t, err)
		wg.Wait()

		assert.Equal(t, table.Options{
			CreateTableRequest: keboola.CreateTableRequest{
				TableDefinition: keboola.TableDefinition{
					PrimaryKeyNames: []string{"id"},
					Columns: []keboola.Column{
						{
							Name: "id",
						},
						{
							Name: "name",
						},
					},
				},
				Name: "table1",
			},
			BucketKey: keboola.BucketKey{
				BranchID: branch.ID,
				BucketID: bucket.BucketID,
			},
		}, res)
	})
}

func getBranchAndBucket() (*keboola.Branch, *keboola.Bucket) {
	branch := &keboola.Branch{
		BranchKey: keboola.BranchKey{
			ID: 123,
		},
		Name:        "testBranch",
		Description: "",
		Created:     iso8601.Time{},
		IsDefault:   false,
	}

	bucket := &keboola.Bucket{
		BucketKey: keboola.BucketKey{
			BranchID: branch.ID,
			BucketID: keboola.BucketID{
				Stage:      keboola.BucketStageIn,
				BucketName: fmt.Sprintf("c-test_%d", 1214124),
			},
		},
	}
	return branch, bucket
}

func TestParseJsonInput(t *testing.T) {
	t.Parallel()
	// Create a temporary directory
	tempDir := t.TempDir()

	// Create a temporary file within the temporary directory
	tempFile, err := os.Create(filepath.Join(tempDir, "foo.json")) // nolint:forbidigo
	require.NoError(t, err)

	defer tempFile.Close()

	// Write content to the temporary file
	_, err = tempFile.Write([]byte(ColumnsInput()))
	require.NoError(t, err)

	// Get the file path of the temporary file
	filePath := tempFile.Name()

	// Read and parse the content of the temporary file
	res, err := ParseJSONInputForCreateTable(filePath)
	require.NoError(t, err)
	assert.Equal(t, []keboola.Column{
		{
			Name: "id",
			Definition: &keboola.ColumnDefinition{
				Type: "INT",
			},
			BaseType: ptr.Ptr(keboola.TypeNumeric),
		},
		{
			Name: "name",
			Definition: &keboola.ColumnDefinition{
				Type: "STRING",
			},
			BaseType: ptr.Ptr(keboola.TypeString),
		},
	}, res)
}

func TestPossiblePrimaryKeys(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name           string
		exceptedResult []string
		columns        keboola.Columns
	}{
		{
			name:           "columns-from definition",
			exceptedResult: []string{"name"},
			columns: keboola.Columns{
				{
					Name: "name",
					Definition: &keboola.ColumnDefinition{
						Type:     "STRING",
						Nullable: false,
					},
					BaseType: ptr.Ptr(keboola.TypeString),
				},
				{
					Name: "age",
					Definition: &keboola.ColumnDefinition{
						Type:     "NUMERIC",
						Nullable: true,
					},
					BaseType: ptr.Ptr(keboola.TypeNumeric),
				},
			},
		},
		{
			name:           "columns flag",
			exceptedResult: []string{"name", "age"},
			columns: keboola.Columns{
				{
					Name: "name",
				},
				{
					Name: "age",
				},
			},
		},
	}

	for _, c := range testCases {
		result := possiblePrimaryKeys(c.columns)
		assert.Equal(t, c.exceptedResult, result)
	}
}

func TestParseOptionsForBigQuery(t *testing.T) {
	t.Parallel()
	// Create a temporary directory
	tempDir := t.TempDir()

	// Create a temporary file within the temporary directory
	tempFile, err := os.Create(filepath.Join(tempDir, "foo.json")) // nolint:forbidigo
	require.NoError(t, err)

	defer tempFile.Close()

	// Write content to the temporary file
	_, err = tempFile.Write([]byte(OptionsInput()))
	require.NoError(t, err)

	// Get the file path of the temporary file
	filePath := tempFile.Name()

	// Read and parse the content of the temporary file
	res, err := parseOptionsFromFile(filePath)
	require.NoError(t, err)
	assert.Equal(t, keboola.TableDefinition{
		TimePartitioning: &keboola.TimePartitioning{
			Type:         keboola.Day,
			ExpirationMs: "864000000",
			Field:        "time",
		},
		RangePartitioning: &keboola.RangePartitioning{
			Field: "id",
			Range: keboola.Range{
				Start:    "0",
				End:      "10",
				Interval: "1",
			},
		},
		Clustering: &keboola.Clustering{
			Fields: []string{
				"id",
			},
		},
	}, res)
}
