package store

import (
	"context"
	"testing"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestStore_CreateSlice(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)
	slice := sliceForTest()

	_, err := store.createSliceOp(ctx, slice).Do(ctx, store.client)
	assert.NoError(t, err)

	// Check keys
	etcdhelper.AssertKVsString(t, store.client, `
<<<<<
slice/active/opened/writing/1000/my-receiver/my-export/2006-01-01T08:04:05.000Z/2006-01-02T08:04:05.000Z
-----
{
  "projectId": 1000,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "2006-01-01T08:04:05.000Z",
  "sliceId": "2006-01-02T08:04:05.000Z",
  "state": "active/opened/writing",
  "mapping": {
    "projectId": 1000,
    "receiverId": "my-receiver",
    "exportId": "my-export",
    "revisionId": 1,
    "tableId": "in.c-bucket.table",
    "incremental": false,
    "columns": [
      {
        "type": "body",
        "name": "body"
      }
    ]
  },
  "storageResource": {
    %A
  },
  "sliceNumber": 1
}
>>>>>
`)
}

func TestStore_GetSliceOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)
	slice := sliceForTest()
	_, err := store.createSliceOp(ctx, slice).Do(ctx, store.client)

	assert.NoError(t, err)

	kv, err := store.getSliceOp(ctx, slice.SliceKey).Do(ctx, store.client)
	assert.NoError(t, err)
	assert.Equal(t, slice, kv.Value)

	// Check keys
	etcdhelper.AssertKVsString(t, store.client, `
<<<<<
slice/active/opened/writing/1000/my-receiver/my-export/2006-01-01T08:04:05.000Z/2006-01-02T08:04:05.000Z
-----
{
  "projectId": 1000,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "2006-01-01T08:04:05.000Z",
  "sliceId": "2006-01-02T08:04:05.000Z",
  "state": "active/opened/writing",
  "mapping": {
    "projectId": 1000,
    "receiverId": "my-receiver",
    "exportId": "my-export",
    "revisionId": 1,
    "tableId": "in.c-bucket.table",
    "incremental": false,
    "columns": [
      {
        "type": "body",
        "name": "body"
      }
    ]
  },
  "storageResource": {
    %A
  },
  "sliceNumber": 1
}
>>>>>
`)
}

func TestStore_ListUploadedSlices(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ExportID: "my-export", ReceiverKey: receiverKey}
	time1, _ := time.Parse(time.RFC3339, "2006-01-01T08:04:05.000Z")
	time2, _ := time.Parse(time.RFC3339, "2006-01-02T08:04:05.000Z")
	time3, _ := time.Parse(time.RFC3339, "2006-01-03T08:04:05.000Z")
	fileKey := key.FileKey{FileID: key.FileID(time1), ExportKey: exportKey}
	mapping := model.Mapping{
		MappingKey: key.MappingKey{
			ExportKey:  exportKey,
			RevisionID: 1,
		},
		TableID: keboola.TableID{
			BucketID: keboola.BucketID{
				Stage:      keboola.BucketStageIn,
				BucketName: "c-bucket",
			},
			TableName: "table",
		},
		Incremental: false,
		Columns: []column.Column{
			column.Body{Name: "body"},
		},
	}
	slice1 := model.NewSlice(fileKey, time2, mapping, 1, &keboola.FileUploadCredentials{})
	slice1.State = slicestate.Uploaded
	slice2 := model.NewSlice(fileKey, time3, mapping, 2, &keboola.FileUploadCredentials{})
	slice2.State = slicestate.Uploaded
	input := []model.Slice{slice1, slice2}

	// Create uploaded slices
	for _, slice := range input {
		_, err := store.schema.Slices().Uploaded().ByKey(slice.SliceKey).PutIfNotExists(slice).Do(ctx, store.client)
		assert.NoError(t, err)
	}

	slices, err := store.ListUploadedSlices(ctx, fileKey)
	assert.NoError(t, err)
	assert.Equal(t, input, slices)

	// Check keys
	etcdhelper.AssertKVsString(t, store.client, `
<<<<<
slice/active/closed/uploaded/1000/my-receiver/my-export/2006-01-01T08:04:05.000Z/2006-01-02T08:04:05.000Z
-----
{
  "projectId": 1000,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "2006-01-01T08:04:05.000Z",
  "sliceId": "2006-01-02T08:04:05.000Z",
  "state": "active/closed/uploaded",
  "mapping": {
    "projectId": 1000,
    "receiverId": "my-receiver",
    "exportId": "my-export",
    "revisionId": 1,
    "tableId": "in.c-bucket.table",
    "incremental": false,
    "columns": [
      {
        "type": "body",
        "name": "body"
      }
    ]
  },
  "storageResource": {
    %A
  },
  "sliceNumber": 1
}
>>>>>

<<<<<
slice/active/closed/uploaded/1000/my-receiver/my-export/2006-01-01T08:04:05.000Z/2006-01-03T08:04:05.000Z
-----
{
  "projectId": 1000,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "2006-01-01T08:04:05.000Z",
  "sliceId": "2006-01-03T08:04:05.000Z",
  "state": "active/closed/uploaded",
  "mapping": {
    "projectId": 1000,
    "receiverId": "my-receiver",
    "exportId": "my-export",
    "revisionId": 1,
    "tableId": "in.c-bucket.table",
    "incremental": false,
    "columns": [
      {
        "type": "body",
        "name": "body"
      }
    ]
  },
  "storageResource": {
    %A
  },
  "sliceNumber": 2
}
>>>>>
`)
}

func sliceForTest() model.Slice {
	time1, _ := time.Parse(time.RFC3339, "2006-01-01T08:04:05.000Z")
	time2, _ := time.Parse(time.RFC3339, "2006-01-02T08:04:05.000Z")
	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ExportID: "my-export", ReceiverKey: receiverKey}
	fileKey := key.FileKey{FileID: key.FileID(time1.UTC()), ExportKey: exportKey}
	mapping := model.Mapping{
		MappingKey: key.MappingKey{
			ExportKey:  exportKey,
			RevisionID: 1,
		},
		TableID: keboola.TableID{
			BucketID: keboola.BucketID{
				Stage:      keboola.BucketStageIn,
				BucketName: "c-bucket",
			},
			TableName: "table",
		},
		Incremental: false,
		Columns: []column.Column{
			column.Body{Name: "body"},
		},
	}
	return model.NewSlice(fileKey, time2, mapping, 1, &keboola.FileUploadCredentials{})
}
