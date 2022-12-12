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

	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "github"}
	exportKey := key.ExportKey{ExportID: "github-issues", ReceiverKey: receiverKey}
	fileID, _ := time.Parse(time.RFC3339, "2006-01-01T15:04:05+07:00")
	fileKey := key.FileKey{FileID: fileID, ExportKey: exportKey}
	file := model.File{
		FileKey: fileKey,
		Mapping: model.Mapping{
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
		},
		StorageResource: &storageapi.File{ID: 1, Name: "file1"},
	}
	_, err := store.createFileOp(ctx, file).Do(ctx, store.client)
	assert.NoError(t, err)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
file/opened/1000/github/github-issues/2006-01-01T08:04:05.000Z
-----
{
  "projectId": 1000,
  "receiverId": "github",
  "exportId": "github-issues",
  "fileId": "2006-01-01T15:04:05+07:00",
  "mapping": {
    "projectId": 1000,
    "receiverId": "github",
    "exportId": "github-issues",
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

	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "github"}
	exportKey := key.ExportKey{ExportID: "github-issues", ReceiverKey: receiverKey}
	time1, _ := time.Parse(time.RFC3339, "2006-01-01T15:04:05+07:00")
	fileKey := key.FileKey{FileID: time1, ExportKey: exportKey}
	input := model.File{
		FileKey: fileKey,
		Mapping: model.Mapping{
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
		},
		StorageResource: &storageapi.File{ID: 2, Name: "file2"},
	}
	_, err := store.createFileOp(ctx, input).Do(ctx, store.client)
	assert.NoError(t, err)

	kv, err := store.getFileOp(ctx, fileKey).Do(ctx, store.client)
	assert.NoError(t, err)
	assert.Equal(t, input, kv.Value)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
file/opened/1000/github/github-issues/2006-01-01T08:04:05.000Z
-----
{
  "projectId": 1000,
  "receiverId": "github",
  "exportId": "github-issues",
  "fileId": "2006-01-01T15:04:05+07:00",
  "mapping": {
    "projectId": 1000,
    "receiverId": "github",
    "exportId": "github-issues",
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
    "id": 2,
    "created": "0001-01-01T00:00:00Z",
    "name": "file2",
    "url": "",
    "provider": "",
    "region": "",
    "maxAgeDays": 0
  }
}
>>>>>
`)
}
