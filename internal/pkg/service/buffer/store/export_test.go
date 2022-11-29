package store

import (
	"context"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestStore_CreateExport(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "github"}
	exportKey := key.ExportKey{ExportID: "github-issues", ReceiverKey: receiverKey}
	mappingKey := key.MappingKey{
		ExportKey:  exportKey,
		RevisionID: 10,
	}
	export := model.Export{
		ExportBase: model.ExportBase{
			ExportKey: exportKey,
			Name:      "Github Issues",
			ImportConditions: model.ImportConditions{
				Count: 5,
				Size:  datasize.MustParseString("50kB"),
				Time:  30 * time.Minute,
			},
		},
		Mapping: model.Mapping{
			MappingKey: mappingKey,
			TableID: model.TableID{
				Stage:  model.TableStageIn,
				Bucket: "bucket",
				Table:  "table",
			},
			Columns: column.Columns{column.Body{}},
		},
		Token: model.Token{Token: "test"},
	}
	err := store.CreateExport(ctx, export)
	assert.NoError(t, err)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
config/export/1000/github/github-issues
-----
{
  "projectId": 1000,
  "receiverId": "github",
  "exportId": "github-issues",
  "name": "Github Issues",
  "importConditions": {
    "count": 5,
    "size": "50KB",
    "time": 1800000000000
  }
}
>>>>>

<<<<<
config/mapping/revision/1000/github/github-issues/00000010
-----
{
  "projectId": 1000,
  "receiverId": "github",
  "exportId": "github-issues",
  "revisionId": 10,
  "tableId": {
    "stage": "in",
    "bucketName": "bucket",
    "tableName": "table"
  },
  "incremental": false,
  "columns": [
    {
      "type": "body"
    }
  ]
}
>>>>>

<<<<<
secret/export/token/1000/github/github-issues
-----
{
  "token": "test",
  "id": "",
  "description": "",
  "isMasterToken": false,
  "canManageBuckets": false,
  "canManageTokens": false,
  "canReadAllFileUploads": false,
  "canPurgeTrash": false,
  "created": "0001-01-01T00:00:00Z",
  "refreshed": "0001-01-01T00:00:00Z",
  "expires": null,
  "isExpired": false,
  "isDisabled": false,
  "owner": {
    "id": 0,
    "name": "",
    "features": null
  }
}
>>>>>
`)
}

func TestStore_ListExports(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "receiver1"}

	// Create exports
	input := []model.Export{
		{
			ExportBase: model.ExportBase{
				ExportKey: key.ExportKey{ExportID: "export-1", ReceiverKey: receiverKey},
				Name:      "Export 1",
				ImportConditions: model.ImportConditions{
					Count: 5,
					Size:  datasize.MustParseString("50kB"),
					Time:  30 * time.Minute,
				},
			},
			Mapping: model.Mapping{
				MappingKey: key.MappingKey{
					ExportKey:  key.ExportKey{ExportID: "export-1", ReceiverKey: receiverKey},
					RevisionID: 10,
				},
				TableID: model.TableID{
					Stage:  model.TableStageIn,
					Bucket: "bucket",
					Table:  "table",
				},
				Columns: column.Columns{column.Body{}},
			},
			Token: model.Token{Token: "test"},
		},
		{
			ExportBase: model.ExportBase{
				ExportKey: key.ExportKey{ExportID: "export-2", ReceiverKey: receiverKey},
				Name:      "Export 2",
				ImportConditions: model.ImportConditions{
					Count: 5,
					Size:  datasize.MustParseString("50kB"),
					Time:  5 * time.Minute,
				},
			},
			Mapping: model.Mapping{
				MappingKey: key.MappingKey{
					ExportKey:  key.ExportKey{ExportID: "export-2", ReceiverKey: receiverKey},
					RevisionID: 10,
				},
				TableID: model.TableID{
					Stage:  model.TableStageIn,
					Bucket: "bucket",
					Table:  "table",
				},
				Columns: column.Columns{column.Body{}},
			},
			Token: model.Token{Token: "test"},
		},
	}

	for _, e := range input {
		err := store.CreateExport(ctx, e)
		assert.NoError(t, err)
	}

	// List
	output, err := store.ListExports(ctx, receiverKey)
	assert.NoError(t, err)
	assert.Equal(t, input, output)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
config/export/1000/receiver1/export-1
-----
{
  "projectId": 1000,
  "receiverId": "receiver1",
  "exportId": "export-1",
  "name": "Export 1",
  "importConditions": {
    "count": 5,
    "size": "50KB",
    "time": 1800000000000
  }
}
>>>>>

<<<<<
config/export/1000/receiver1/export-2
-----
{
  "projectId": 1000,
  "receiverId": "receiver1",
  "exportId": "export-2",
  "name": "Export 2",
  "importConditions": {
    "count": 5,
    "size": "50KB",
    "time": 300000000000
  }
}
>>>>>

<<<<<
config/mapping/revision/1000/receiver1/export-1/00000010
-----
{
  "projectId": 1000,
  "receiverId": "receiver1",
  "exportId": "export-1",
  "revisionId": 10,
  "tableId": {
    "stage": "in",
    "bucketName": "bucket",
    "tableName": "table"
  },
  "incremental": false,
  "columns": [
    {
      "type": "body"
    }
  ]
}
>>>>>

<<<<<
config/mapping/revision/1000/receiver1/export-2/00000010
-----
{
  "projectId": 1000,
  "receiverId": "receiver1",
  "exportId": "export-2",
  "revisionId": 10,
  "tableId": {
    "stage": "in",
    "bucketName": "bucket",
    "tableName": "table"
  },
  "incremental": false,
  "columns": [
    {
      "type": "body"
    }
  ]
}
>>>>>

<<<<<
secret/export/token/1000/receiver1/export-1
-----
{
  "token": "test",
  "id": "",
  "description": "",
  "isMasterToken": false,
  "canManageBuckets": false,
  "canManageTokens": false,
  "canReadAllFileUploads": false,
  "canPurgeTrash": false,
  "created": "0001-01-01T00:00:00Z",
  "refreshed": "0001-01-01T00:00:00Z",
  "expires": null,
  "isExpired": false,
  "isDisabled": false,
  "owner": {
    "id": 0,
    "name": "",
    "features": null
  }
}
>>>>>

<<<<<
secret/export/token/1000/receiver1/export-2
-----
{
  "token": "test",
  "id": "",
  "description": "",
  "isMasterToken": false,
  "canManageBuckets": false,
  "canManageTokens": false,
  "canReadAllFileUploads": false,
  "canPurgeTrash": false,
  "created": "0001-01-01T00:00:00Z",
  "refreshed": "0001-01-01T00:00:00Z",
  "expires": null,
  "isExpired": false,
  "isDisabled": false,
  "owner": {
    "id": 0,
    "name": "",
    "features": null
  }
}
>>>>>
`)
}
