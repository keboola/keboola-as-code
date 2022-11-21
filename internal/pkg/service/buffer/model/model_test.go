package model_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model"
)

func TestMappedColumns(t *testing.T) {
	t.Parallel()

	input := model.MappedColumns{
		model.ColumnID{},
		model.ColumnDatetime{},
		model.ColumnIP{},
		model.ColumnBody{},
		model.ColumnHeaders{},
		model.ColumnTemplate{
			Language:               "jsonnet",
			UndefinedValueStrategy: "null",
			Content:                `body.my.key+":"+body.my.value`,
			DataType:               "STRING",
		},
	}

	bytes, err := json.Marshal(&input)
	assert.NoError(t, err)

	var output model.MappedColumns
	err = json.Unmarshal(bytes, &output)
	assert.NoError(t, err)

	assert.Equal(t, input, output)
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
