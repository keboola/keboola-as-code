package model_test

import (
	"encoding/json"
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/stretchr/testify/assert"
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
