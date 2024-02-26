package dialog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ColumnsInput() string {
	return `[{"name": "id","definition": {"type": "INT"},"basetype": "NUMERIC"},{"name": "name","definition": {"type": "STRING"},"basetype": "STRING"}]`
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
	res, err := parseJSONInputForCreateTable(filePath)
	require.NoError(t, err)
	assert.Equal(t, []keboola.Column{
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
	}, res)
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
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equalf(t, tt.want, getOptionCreateRequest(tt.args.columns), "getOptionCreateRequest(%v)", tt.args.columns)
		})
	}
}
