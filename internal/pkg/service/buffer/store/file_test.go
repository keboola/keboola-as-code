package store

import (
	"context"
	"testing"
	"time"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestStore_CreateFile(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)
	file := newFileForTest()

	_, err := store.createFileOp(ctx, file).Do(ctx, store.client)
	assert.NoError(t, err)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
file/opened/1000/my-receiver/my-export/2006-01-01T08:04:05.000Z
-----
{
  "projectId": 1000,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "2006-01-01T15:04:05+07:00",
  "mapping": {
    "projectId": 1000,
    "receiverId": "my-receiver",
    "exportId": "my-export",
    "revisionId": 1,
    "tableId": {
      "stage": "in",
      "bucketName": "bucket",
      "tableName": "table"
    },
    "incremental": false,
    "columns": [
      {
        "type": "body",
        "name": "body"
      }
    ]
  },
  "storageResource": {
    "id": 1,
    "created": "0001-01-01T00:00:00Z",
    "name": "file1",
    "url": "",
    "provider": "",
    "region": "",
    "maxAgeDays": 0
  }
}
>>>>>
`)
}

func TestStore_GetFileOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)
	file := newFileForTest()

	_, err := store.createFileOp(ctx, file).Do(ctx, store.client)
	assert.NoError(t, err)

	kv, err := store.getFileOp(ctx, file.FileKey).Do(ctx, store.client)
	assert.NoError(t, err)
	assert.Equal(t, file, kv.Value)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
file/opened/1000/my-receiver/my-export/2006-01-01T08:04:05.000Z
-----
{
  "projectId": 1000,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "2006-01-01T15:04:05+07:00",
  "mapping": {
    "projectId": 1000,
    "receiverId": "my-receiver",
    "exportId": "my-export",
    "revisionId": 1,
    "tableId": {
      "stage": "in",
      "bucketName": "bucket",
      "tableName": "table"
    },
    "incremental": false,
    "columns": [
      {
        "type": "body",
        "name": "body"
      }
    ]
  },
  "storageResource": {
    "id": 1,
    "created": "0001-01-01T00:00:00Z",
    "name": "file1",
    "url": "",
    "provider": "",
    "region": "",
    "maxAgeDays": 0
  }
}
>>>>>
`)
}

func newFileForTest() model.File {
	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ExportID: "my-export", ReceiverKey: receiverKey}
	now, _ := time.Parse(time.RFC3339, "2006-01-01T15:04:05+07:00")
	mapping := model.Mapping{
		MappingKey: key.MappingKey{
			ExportKey:  exportKey,
			RevisionID: 1,
		},
		TableID: model.TableID{
			Stage:  "in",
			Bucket: "bucket",
			Table:  "table",
		},
		Incremental: false,
		Columns: []column.Column{
			column.Body{Name: "body"},
		},
	}
	resource := &storageapi.File{ID: 1, Name: "file1"}
	return model.NewFile(exportKey, now, mapping, resource)
}
