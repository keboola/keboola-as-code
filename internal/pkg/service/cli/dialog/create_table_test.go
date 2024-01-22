package dialog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/table"
)

var testInput = `{"name": "table1","primaryKeysNames": ["id"],"columns": [{"name": "id","definition": {"type": "INT"},"basetype": "NUMERIC"},{"name": "name","definition": {"type": "STRING"},"basetype": "STRING"}]}`

func TestParseJsonInput(t *testing.T) {
	t.Parallel()
	// Create a temporary directory
	tempDir := t.TempDir()

	// Create a temporary file within the temporary directory
	tempFile, err := os.Create(filepath.Join(tempDir, "foo.json")) // nolint:forbidigo
	require.NoError(t, err)

	defer tempFile.Close()

	// Write content to the temporary file
	_, err = tempFile.Write([]byte(testInput))
	require.NoError(t, err)

	// Get the file path of the temporary file
	filePath := tempFile.Name()

	// Read and parse the content of the temporary file
	res, err := parseJSONInputForCreateTable(filePath)
	require.NoError(t, err)
	assert.Equal(t, &keboola.CreateTableRequest{
		TableDefinition: keboola.TableDefinition{
			PrimaryKeyNames: []string{"id"},
			Columns: []keboola.Column{
				{
					Name: "id",
					Definition: keboola.ColumnDefinition{
						Type: "INT",
					},
					BaseType: "NUMERIC",
				},
				{
					Name: "name",
					Definition: keboola.ColumnDefinition{
						Type: "STRING",
					},
					BaseType: "STRING",
				},
			},
		},
		Name: "table1",
	}, res)
}

func TestGetCreateRequest(t *testing.T) {
	t.Parallel()
	type args struct {
		opts table.Options
	}
	tests := []struct {
		name string
		args args
		want table.Options
	}{
		{
			name: "getCreateTableRequest",
			args: args{opts: table.Options{
				CreateTableRequest: keboola.CreateTableRequest{},
				BucketKey:          keboola.BucketKey{},
				Columns:            []string{"id", "name"},
				Name:               "test_table",
				PrimaryKey:         []string{"id"},
			}}, want: table.Options{
				CreateTableRequest: keboola.CreateTableRequest{
					TableDefinition: keboola.TableDefinition{
						PrimaryKeyNames: []string{"id"},
						Columns: []keboola.Column{
							{
								Name: "id",
								Definition: keboola.ColumnDefinition{
									Type: "STRING",
								},
								BaseType: keboola.TypeString,
							},
							{
								Name: "name",
								Definition: keboola.ColumnDefinition{
									Type: "STRING",
								},
								BaseType: keboola.TypeString,
							},
						},
					},
					Name: "test_table",
				},
				BucketKey:  keboola.BucketKey{},
				Columns:    []string{"id", "name"},
				Name:       "test_table",
				PrimaryKey: []string{"id"},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equalf(t, tt.want, getOptionCreateRequest(tt.args.opts), "getOptionCreateRequest(%v)", tt.args.opts)
		})
	}
}
