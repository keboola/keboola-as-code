package model_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model"
)

func TestTableID_String(t *testing.T) {
	t.Parallel()

	tableID := model.TableID{
		Stage:      model.TableStageIn,
		BucketName: "main",
		TableName:  "table1",
	}
	assert.Equal(t, "in.c-main.table1", tableID.String())
}
