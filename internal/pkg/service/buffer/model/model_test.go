package model_test

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model"
	"github.com/stretchr/testify/assert"
)

func TestTableID_String(t *testing.T) {
	t.Parallel()

	tableID := model.TableID{
		Stage:      "in",
		BucketName: "main",
		TableName:  "table1",
	}
	assert.Equal(t, "in.c-main.table1", tableID.String())
}
