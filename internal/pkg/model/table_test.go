package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableID_String(t *testing.T) {
	t.Parallel()

	tableID := TableID{
		Stage:      "in",
		BucketName: "main",
		TableName:  "table1",
	}
	assert.Equal(t, "in.c-main.table1", tableID.String())
}
