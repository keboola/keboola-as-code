package column_test

import (
	"encoding/json"
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model/column"
	"github.com/stretchr/testify/assert"
)

func TestMappedColumns(t *testing.T) {
	t.Parallel()

	typed := column.Columns{
		column.ID{},
		column.Datetime{},
		column.IP{},
		column.Body{},
		column.Headers{},
		column.ColumnTemplate{
			Language:               "jsonnet",
			UndefinedValueStrategy: "null",
			Content:                `body.my.key+":"+body.my.value`,
			DataType:               "STRING",
		},
	}
	untyped := `[{"type":"id"},{"type":"datetime"},{"type":"ip"},{"type":"body"},{"type":"headers"},{"type":"template","language":"jsonnet","undefinedValueStrategy":"null","content":"body.my.key+\":\"+body.my.value","dataType":"STRING"}]`

	bytes, err := json.Marshal(&typed)
	assert.NoError(t, err)
	assert.Equal(t, untyped, string(bytes))

	var output column.Columns
	err = json.Unmarshal(bytes, &output)
	assert.NoError(t, err)
	assert.Equal(t, typed, output)
}

func TestMappedColumns_Error(t *testing.T) {
	t.Parallel()

	// Unmarshal unknown type
	var output column.Columns
	err := json.Unmarshal([]byte(`[{"type":"unknown"}]`), &output)
	assert.Error(t, err, `invalid column type name "unknown"`)
}
