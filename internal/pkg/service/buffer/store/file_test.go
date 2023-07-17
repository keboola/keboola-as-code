package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
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
	etcdhelper.AssertKVsString(t, store.client, `
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
	etcdhelper.AssertKVsString(t, store.client, `
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
		{filestate.Closing, filestate.Importing},
		{filestate.Importing, filestate.Failed},
		{filestate.Failed, filestate.Importing},
		{filestate.Importing, filestate.Imported},
	}

	ctx := context.Background()
	store := newStoreForTest(t)
	file := newFileForTest()
	now, _ := time.Parse(time.RFC3339, "2010-01-01T01:01:01+07:00")

	// Create file
	_, err := store.createFileOp(ctx, file).Do(ctx, store.client)
	assert.NoError(t, err)

	for _, tc := range testCases {
		// Trigger transition
		ok, err := store.SetFileState(ctx, now, &file, tc.to)
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
  "<STATE>At": "2009-12-31T18:01:01.000Z"%A
>>>>>
`
		etcdhelper.AssertKVsString(t, store.client, strings.ReplaceAll(expected, "<STATE>", tc.to.String()))

		// Test duplicated transition -> nop
		file.State = tc.from
		ok, err = store.SetFileState(ctx, time.Now(), &file, tc.to)
		assert.Error(t, err, desc)
		assert.Equal(t, fmt.Sprintf(`file "%s" is already in the "%s" state`, file.FileKey, tc.to), err.Error())
		assert.False(t, ok, desc)
		assert.Equal(t, tc.to, file.State, desc)
	}
}

func newFileForTest() model.File {
	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ExportID: "my-export", ReceiverKey: receiverKey}
	now, _ := time.Parse(time.RFC3339, "2006-01-01T08:04:05.000Z")
	mapping := mappingForTest(exportKey)
	resource := &keboola.FileUploadCredentials{File: keboola.File{ID: 1, Name: "file1"}}
	return model.NewFile(exportKey, now, mapping, resource)
}

func mappingForTest(exportKey key.ExportKey) model.Mapping {
	return model.Mapping{
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
}
