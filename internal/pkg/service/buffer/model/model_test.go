package model_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model"
)

func TestMappedColumns(t *testing.T) {
	t.Parallel()

	typed := model.Columns{
		model.ID{},
		model.Datetime{},
		model.IP{},
		model.Body{},
		model.Headers{},
		model.ColumnTemplate{
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

	var output model.Columns
	err = json.Unmarshal(bytes, &output)
	assert.NoError(t, err)
	assert.Equal(t, typed, output)
}

func TestMappedColumns_Error(t *testing.T) {
	t.Parallel()

	// Unmarshal unknown type
	var output model.Columns
	err := json.Unmarshal([]byte(`[{"type":"unknown"}]`), &output)
	assert.Error(t, err, `invalid column type name "unknown"`)
}

func TestTableID_String(t *testing.T) {
	t.Parallel()

	tableID := model.TableID{
		Stage:      "in",
		BucketName: "main",
		TableName:  "table1",
	}
	assert.Equal(t, "in.c-main.table1", tableID.String())
}
