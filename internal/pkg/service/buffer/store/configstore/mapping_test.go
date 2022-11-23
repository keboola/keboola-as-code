package configstore

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestConfigStore_GetCurrentMapping(t *testing.T) {
	t.Parallel()

	// Setup
	ctx, d := newTestDeps(t)
	store := New(d.logger, d.etcdClient, d.validator, d.tracer)

	projectID := 1000
	receiverID := "receiver1"
	exportID := "export1"
	tableID := model.TableID{
		Stage:  model.TableStageIn,
		Bucket: "main",
		Table:  "table1",
	}

	// Create mapppings
	input := []model.Mapping{
		{
			RevisionID:  1,
			TableID:     tableID,
			Incremental: false,
			Columns:     column.Columns{column.ID{}},
		},
		{
			RevisionID:  2,
			TableID:     tableID,
			Incremental: false,
			Columns:     column.Columns{column.ID{}},
		},
		{
			RevisionID:  10,
			TableID:     tableID,
			Incremental: true,
			Columns:     column.Columns{column.ID{}},
		},
	}

	for _, m := range input {
		err := store.CreateMapping(ctx, projectID, receiverID, exportID, m)
		assert.NoError(t, err)
	}

	// Get current mapping
	mapping, err := store.GetCurrentMapping(ctx, projectID, receiverID, exportID)
	assert.NoError(t, err)
	assert.Equal(t, &input[2], mapping)

	// Check keys
	etcdhelper.AssertKVs(t, d.etcdClient, `
<<<<<
config/mapping/revision/1000/receiver1/export1/00000001
-----
{
  "revisionId": 1,
  "tableId": {
    "stage": "in",
    "bucketName": "main",
    "tableName": "table1"
  },
  "incremental": false,
  "columns": [
    {
      "type": "id"
    }
  ]
}
>>>>>

<<<<<
config/mapping/revision/1000/receiver1/export1/00000002
-----
{
  "revisionId": 2,
  "tableId": {
    "stage": "in",
    "bucketName": "main",
    "tableName": "table1"
  },
  "incremental": false,
  "columns": [
    {
      "type": "id"
    }
  ]
}
>>>>>

<<<<<
config/mapping/revision/1000/receiver1/export1/00000010
-----
{
  "revisionId": 10,
  "tableId": {
    "stage": "in",
    "bucketName": "main",
    "tableName": "table1"
  },
  "incremental": true,
  "columns": [
    {
      "type": "id"
    }
  ]
}
>>>>>
`)
}
