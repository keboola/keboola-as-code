package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
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
  "fileId": "2006-01-01T08:04:05.000Z",
  "state": "opened",
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
  "fileId": "2006-01-01T08:04:05.000Z",
  "state": "opened",
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

func TestStore_SetFileState_Transitions(t *testing.T) {
	t.Parallel()

	// Test all transitions
	testCases := []struct{ from, to filestate.State }{
		{filestate.Opened, filestate.Closing},
		{filestate.Closing, filestate.Closed},
		{filestate.Closed, filestate.Importing},
		{filestate.Importing, filestate.Failed},
		{filestate.Failed, filestate.Importing},
		{filestate.Importing, filestate.Imported},
	}

	ctx := context.Background()
	store := newStoreForTest(t)
	file := newFileForTest()
	now, _ := time.Parse(time.RFC3339, "2010-01-01T01:01:01+07:00")

	// Create file
	assert.NoError(t, store.CreateFile(ctx, file))

	for _, tc := range testCases {
		// Trigger transition
		ok, err := store.SetFileState(ctx, &file, tc.to, now)
		desc := fmt.Sprintf("%s -> %s", tc.from, tc.to)
		assert.NoError(t, err, desc)
		assert.True(t, ok, desc)
		assert.Equal(t, tc.to, file.State, desc)
		expected := `
<<<<<
file/<STATE>/1000/my-receiver/my-export/2006-01-01T08:04:05.000Z
-----
%A
  "state": "<STATE>",%A
  "<STATE>At": "2010-01-01T01:01:01+07:00"%A
>>>>>
`
		etcdhelper.AssertKVs(t, store.client, strings.ReplaceAll(expected, "<STATE>", tc.to.String()))

		// Test duplicated transition -> nop
		file.State = tc.from
		ok, err = store.SetFileState(ctx, &file, tc.to, time.Now())
		assert.NoError(t, err, desc)
		assert.False(t, ok, desc)
		assert.Equal(t, tc.to, file.State, desc)
	}
}

func newFileForTest() model.File {
	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ExportID: "my-export", ReceiverKey: receiverKey}
	now, _ := time.Parse(time.RFC3339, "2006-01-01T08:04:05.000Z")
	mapping := model.Mapping{
		MappingKey: key.MappingKey{
			ExportKey:  exportKey,
			RevisionID: 1,
		},
		TableID: storageapi.TableID{
			BucketID: storageapi.BucketID{
				Stage:      storageapi.BucketStageIn,
				BucketName: "bucket",
			},
			TableName: "table",
		},
		Incremental: false,
		Columns: []column.Column{
			column.Body{Name: "body"},
		},
	}
	resource := &storageapi.File{ID: 1, Name: "file1"}
	return model.NewFile(exportKey, now, mapping, resource)
}
