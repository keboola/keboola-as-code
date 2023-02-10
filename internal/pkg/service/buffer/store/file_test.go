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
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
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
file/opened/00001000/my-receiver/my-export/2006-01-01T08:04:05.000Z
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
file/opened/00001000/my-receiver/my-export/2006-01-01T08:04:05.000Z
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
file/<STATE>/00001000/my-receiver/my-export/2006-01-01T08:04:05.000Z
-----
%A
  "state": "<STATE>",%A
  "<STATE>At": "2009-12-31T18:01:01.000Z"%A
>>>>>
`
		etcdhelper.AssertKVs(t, store.client, strings.ReplaceAll(expected, "<STATE>", tc.to.String()))

		// Test duplicated transition -> nop
		file.State = tc.from
		ok, err = store.SetFileState(ctx, time.Now(), &file, tc.to)
		assert.NoError(t, err, desc)
		assert.False(t, ok, desc)
		assert.Equal(t, tc.to, file.State, desc)
	}
}

func TestStore_ListFilesInStateWithEnd(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newStoreForTest(t)

	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ExportID: "my-export", ReceiverKey: receiverKey}
	mapping := mappingForTest(exportKey)
	resource := &keboola.File{ID: 1, Name: "file1"}
	now := time.Now()

	// Create a file before the boundary time in the right state - will be listed
	time1 := now.Add(-30 * 24 * time.Hour)
	file1 := model.NewFile(exportKey, time1, mapping, resource)
	_, err := store.createFileOp(ctx, file1).Do(ctx, store.client)
	assert.NoError(t, err)

	// Create a file before the boundary time in the right state - will be listed
	time2 := now.Add(-20 * 24 * time.Hour)
	file2 := model.NewFile(exportKey, time2, mapping, resource)
	_, err = store.createFileOp(ctx, file2).Do(ctx, store.client)
	assert.NoError(t, err)

	// Create a file before the boundary time in the wrong state - won't be listed
	time3 := now.Add(-19 * 24 * time.Hour)
	file3 := model.NewFile(exportKey, time3, mapping, resource)
	_, err = store.createFileOp(ctx, file3).Do(ctx, store.client)
	assert.NoError(t, err)
	ok, err := store.SetFileState(ctx, time3, &file3, filestate.Closing)
	assert.NoError(t, err)
	assert.True(t, ok)

	// Create a file after the boundary time in the right state - won't be listed
	time4 := now.Add(-1 * 24 * time.Hour)
	file4 := model.NewFile(exportKey, time4, mapping, resource)
	_, err = store.createFileOp(ctx, file4).Do(ctx, store.client)
	assert.NoError(t, err)

	// List files older than 14 days
	boundaryKey := key.UTCTime(now.Add(-14 * 24 * time.Hour)).String()
	files, err := store.ListFilesInState(ctx, filestate.Opened, exportKey, iterator.WithEnd(boundaryKey))
	assert.NoError(t, err)
	assert.Len(t, files, 2)
	assert.Equal(t, files[0].FileID.String(), file1.FileID.String())
	assert.Equal(t, files[1].FileID.String(), file2.FileID.String())
}

func newFileForTest() model.File {
	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ExportID: "my-export", ReceiverKey: receiverKey}
	now, _ := time.Parse(time.RFC3339, "2006-01-01T08:04:05.000Z")
	mapping := mappingForTest(exportKey)
	resource := &keboola.File{ID: 1, Name: "file1"}
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
