package store

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestStore_Mapping_Ops(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "receiver1"}
	exportKey := key.ExportKey{ReceiverKey: receiverKey, ExportID: "export1"}
	tableID := storageapi.TableID{
		BucketID: storageapi.BucketID{
			Stage:      storageapi.BucketStageIn,
			BucketName: "main",
		},
		TableName: "table1",
	}

	// Create mapppings
	input := []model.Mapping{
		{
			MappingKey:  key.MappingKey{RevisionID: 1, ExportKey: exportKey},
			TableID:     tableID,
			Incremental: false,
			Columns:     column.Columns{column.ID{Name: "id"}},
		},
		{
			MappingKey:  key.MappingKey{RevisionID: 2, ExportKey: exportKey},
			TableID:     tableID,
			Incremental: false,
			Columns:     column.Columns{column.ID{Name: "id"}},
		},
		{
			MappingKey:  key.MappingKey{RevisionID: 10, ExportKey: exportKey},
			TableID:     tableID,
			Incremental: true,
			Columns:     column.Columns{column.ID{Name: "id"}},
		},
	}

	for _, m := range input {
		_, err := store.createMappingOp(ctx, m).Do(ctx, store.client)
		assert.NoError(t, err)
	}

	// Get current mapping
	mapping, err := store.GetLatestMapping(ctx, exportKey)
	assert.NoError(t, err)
	assert.Equal(t, input[2], mapping)

	// Get mapping 1 by RevisionID
	mapping, err = store.GetMapping(ctx, input[0].MappingKey)
	assert.NoError(t, err)
	assert.Equal(t, input[0], mapping)

	// Get mapping 2 by RevisionID
	mapping, err = store.GetMapping(ctx, input[1].MappingKey)
	assert.NoError(t, err)
	assert.Equal(t, input[1], mapping)

	// Get mapping 10 by RevisionID
	mapping, err = store.GetMapping(ctx, input[2].MappingKey)
	assert.NoError(t, err)
	assert.Equal(t, input[2], mapping)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
config/mapping/revision/1000/receiver1/export1/00000001
-----
{
  "projectId": 1000,
  "receiverId": "receiver1",
  "exportId": "export1",
  "revisionId": 1,
  "tableId": "in.c-main.table1",
  "incremental": false,
  "columns": [
    {
      "type": "id",
      "name": "id"
    }
  ]
}
>>>>>

<<<<<
config/mapping/revision/1000/receiver1/export1/00000002
-----
{
  "projectId": 1000,
  "receiverId": "receiver1",
  "exportId": "export1",
  "revisionId": 2,
  "tableId": "in.c-main.table1",
  "incremental": false,
  "columns": [
    {
      "type": "id",
      "name": "id"
    }
  ]
}
>>>>>

<<<<<
config/mapping/revision/1000/receiver1/export1/00000010
-----
{
  "projectId": 1000,
  "receiverId": "receiver1",
  "exportId": "export1",
  "revisionId": 10,
  "tableId": "in.c-main.table1",
  "incremental": true,
  "columns": [
    {
      "type": "id",
      "name": "id"
    }
  ]
}
>>>>>
`)

	_, err = store.deleteExportMappingsOp(ctx, exportKey).Do(ctx, store.client)
	assert.NoError(t, err)

	etcdhelper.AssertKVs(t, store.client, ``)
}

func TestStore_DeleteReceiverMappingsOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	export1 := key.ExportKey{ReceiverKey: key.ReceiverKey{ProjectID: 1000, ReceiverID: "receiver1"}, ExportID: "export1"}
	export0 := key.ExportKey{ReceiverKey: key.ReceiverKey{ProjectID: 1000, ReceiverID: "receiver0"}, ExportID: "export0"}
	tableID := storageapi.TableID{
		BucketID: storageapi.BucketID{
			Stage:      storageapi.BucketStageIn,
			BucketName: "main",
		},
		TableName: "table1",
	}

	// Create mapppings
	input := []model.Mapping{
		{
			MappingKey:  key.MappingKey{RevisionID: 1, ExportKey: export0},
			TableID:     tableID,
			Incremental: false,
			Columns:     column.Columns{column.ID{Name: "id"}},
		},
		{
			MappingKey:  key.MappingKey{RevisionID: 2, ExportKey: export1},
			TableID:     tableID,
			Incremental: false,
			Columns:     column.Columns{column.ID{Name: "id"}},
		},
	}

	for _, m := range input {
		_, err := store.createMappingOp(ctx, m).Do(ctx, store.client)
		assert.NoError(t, err)
	}

	etcdhelper.AssertKVs(t, store.client, `
<<<<<
config/mapping/revision/1000/receiver0/export0/00000001
-----
{
  "projectId": 1000,
  "receiverId": "receiver0",
  "exportId": "export0",
  "revisionId": 1,
  "tableId": "in.c-main.table1",
  "incremental": false,
  "columns": [
    {
      "type": "id",
      "name": "id"
    }
  ]
}
>>>>>

<<<<<
config/mapping/revision/1000/receiver1/export1/00000002
-----
{
  "projectId": 1000,
  "receiverId": "receiver1",
  "exportId": "export1",
  "revisionId": 2,
  "tableId": "in.c-main.table1",
  "incremental": false,
  "columns": [
    {
      "type": "id",
      "name": "id"
    }
  ]
}
>>>>>
`)

	_, err := store.deleteReceiverMappingsOp(ctx, export0.ReceiverKey).Do(ctx, store.client)
	assert.NoError(t, err)

	etcdhelper.AssertKVs(t, store.client, `
<<<<<
config/mapping/revision/1000/receiver1/export1/00000002
-----
{
  "projectId": 1000,
  "receiverId": "receiver1",
  "exportId": "export1",
  "revisionId": 2,
  "tableId": "in.c-main.table1",
  "incremental": false,
  "columns": [
    {
      "type": "id",
      "name": "id"
    }
  ]
}
>>>>>
`)

	_, err = store.deleteReceiverMappingsOp(ctx, export1.ReceiverKey).Do(ctx, store.client)
	assert.NoError(t, err)

	etcdhelper.AssertKVs(t, store.client, ``)
}
