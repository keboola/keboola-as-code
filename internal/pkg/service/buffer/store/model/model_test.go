package model_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
)

func TestTableID_String(t *testing.T) {
	t.Parallel()

	tableID := model.TableID{
		Stage:  model.TableStageIn,
		Bucket: "main",
		Table:  "table1",
	}
	s := tableID.String()
	assert.Equal(t, "in.c-main.table1", s)

	parsed, err := model.ParseTableID(s)
	assert.NoError(t, err)
	assert.Equal(t, tableID, parsed)
}
