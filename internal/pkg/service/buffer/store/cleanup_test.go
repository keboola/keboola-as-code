package store

import (
	"context"
	"testing"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestStore_Cleanup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "github"}
	exportKey1 := key.ExportKey{ExportID: "first", ReceiverKey: receiverKey}
	exportKey2 := key.ExportKey{ExportID: "another", ReceiverKey: receiverKey}
	exportKey3 := key.ExportKey{ExportID: "third", ReceiverKey: receiverKey}
	receiver := model.Receiver{
		ReceiverBase: model.ReceiverBase{
			ReceiverKey: receiverKey,
			Name:        "rec1",
			Secret:      "sec1",
		},
		Exports: []model.Export{
			{
				ExportBase: model.ExportBase{
					ExportKey: exportKey1,
				},
			},
			{
				ExportBase: model.ExportBase{
					ExportKey: exportKey2,
				},
			},
			{
				ExportBase: model.ExportBase{
					ExportKey: exportKey3,
				},
			},
		},
	}

	// Add task without a finishedAt timestamp but too old - will be deleted
	time1, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05+07:00")
	taskKey1 := key.TaskKey{ReceiverKey: receiverKey, Type: "some.task", TaskID: key.UTCTime(time1).String() + "_abcdef"}
	task1 := model.Task{
		CreatedAt:  key.UTCTime(time1),
		TaskKey:    taskKey1,
		FinishedAt: nil,
		WorkerNode: "node1",
		Lock:       "lock1",
		Result:     "",
		Error:      "err",
		Duration:   nil,
	}
	err := store.schema.Tasks().ByKey(taskKey1).Put(task1).Do(ctx, store.client)
	assert.NoError(t, err)

	// Add task with a finishedAt timestamp in the past - will be deleted
	time2, _ := time.Parse(time.RFC3339, "2008-01-02T15:04:05+07:00")
	time2Key := key.UTCTime(time2)
	taskKey2 := key.TaskKey{ReceiverKey: receiverKey, Type: "other.task", TaskID: key.UTCTime(time1).String() + "_ghijkl"}
	task2 := model.Task{
		CreatedAt:  key.UTCTime(time1),
		TaskKey:    taskKey2,
		FinishedAt: &time2Key,
		WorkerNode: "node2",
		Lock:       "lock2",
		Result:     "res",
		Error:      "",
		Duration:   nil,
	}
	err = store.schema.Tasks().ByKey(taskKey2).Put(task2).Do(ctx, store.client)
	assert.NoError(t, err)

	// Add task with a finishedAt timestamp before a moment - will be ignored
	time3 := time.Now()
	time3Key := key.UTCTime(time3)
	taskKey3 := key.TaskKey{ReceiverKey: receiverKey, Type: "other.task", TaskID: key.UTCTime(time1).String() + "_ghijkl"}
	task3 := model.Task{
		CreatedAt:  key.UTCTime(time1),
		TaskKey:    taskKey3,
		FinishedAt: &time3Key,
		WorkerNode: "node2",
		Lock:       "lock2",
		Result:     "res",
		Error:      "",
		Duration:   nil,
	}
	err = store.schema.Tasks().ByKey(taskKey3).Put(task3).Do(ctx, store.client)
	assert.NoError(t, err)

	// Add file with an Opened state and created in the past - will be deleted
	fileKey1 := key.FileKey{ExportKey: exportKey1, FileID: key.FileID(time1)}
	file1 := model.File{
		FileKey: fileKey1,
		State:   filestate.Opened,
		Mapping: model.Mapping{
			MappingKey:  key.MappingKey{ExportKey: exportKey1, RevisionID: 1},
			TableID:     keboola.TableID{BucketID: keboola.BucketID{Stage: "in", BucketName: "test"}, TableName: "test"},
			Incremental: false,
			Columns:     []column.Column{column.ID{Name: "id", PrimaryKey: false}},
		},
		StorageResource: &keboola.File{ID: 123, Name: "file1.csv"},
	}
	err = store.schema.Files().InState(filestate.Opened).ByKey(fileKey1).Put(file1).Do(ctx, store.client)
	assert.NoError(t, err)

	// Add file with an Opened state and created recently - will be ignored
	fileKey2 := key.FileKey{ExportKey: exportKey3, FileID: key.FileID(time3)}
	file2 := model.File{
		FileKey: fileKey2,
		State:   filestate.Opened,
		Mapping: model.Mapping{
			MappingKey:  key.MappingKey{ExportKey: exportKey3, RevisionID: 1},
			TableID:     keboola.TableID{BucketID: keboola.BucketID{Stage: "in", BucketName: "test"}, TableName: "test"},
			Incremental: false,
			Columns:     []column.Column{column.ID{Name: "id", PrimaryKey: false}},
		},
		StorageResource: &keboola.File{ID: 123, Name: "file1.csv"},
	}
	err = store.schema.Files().InState(filestate.Opened).ByKey(fileKey2).Put(file2).Do(ctx, store.client)
	assert.NoError(t, err)

	// Add slice for the cleaned-up file - will be deleted
	sliceKey1 := key.SliceKey{FileKey: fileKey1, SliceID: key.SliceID(time1)}
	slice1 := model.Slice{
		SliceKey: sliceKey1,
		Number:   1,
		State:    slicestate.Imported,
		Mapping: model.Mapping{
			MappingKey:  key.MappingKey{ExportKey: exportKey1, RevisionID: 1},
			TableID:     keboola.TableID{BucketID: keboola.BucketID{Stage: "in", BucketName: "test"}, TableName: "test"},
			Incremental: false,
			Columns:     []column.Column{column.ID{Name: "id", PrimaryKey: false}},
		},
		StorageResource: &keboola.File{ID: 123, Name: "file1.csv"},
	}
	err = store.schema.Slices().InState(slicestate.Imported).ByKey(sliceKey1).Put(slice1).Do(ctx, store.client)
	assert.NoError(t, err)

	// Add slice for the ignored file - will be ignored
	sliceKey2 := key.SliceKey{FileKey: fileKey2, SliceID: key.SliceID(time3)}
	slice2 := model.Slice{
		SliceKey: sliceKey2,
		Number:   1,
		State:    slicestate.Imported,
		Mapping: model.Mapping{
			MappingKey:  key.MappingKey{ExportKey: exportKey3, RevisionID: 1},
			TableID:     keboola.TableID{BucketID: keboola.BucketID{Stage: "in", BucketName: "test"}, TableName: "test"},
			Incremental: false,
			Columns:     []column.Column{column.ID{Name: "id", PrimaryKey: false}},
		},
		StorageResource: &keboola.File{ID: 123, Name: "file1.csv"},
	}
	err = store.schema.Slices().InState(slicestate.Imported).ByKey(sliceKey2).Put(slice2).Do(ctx, store.client)
	assert.NoError(t, err)

	// Add record for the cleaned-up slice - will be deleted
	recordKey1 := key.RecordKey{SliceKey: sliceKey1, ReceivedAt: key.ReceivedAt(time1), RandomSuffix: "abcd"}
	err = store.schema.Records().ByKey(recordKey1).Put("rec").Do(ctx, store.client)
	assert.NoError(t, err)

	// Add record for the ignored slice - will be ignored
	recordKey2 := key.RecordKey{SliceKey: sliceKey2, ReceivedAt: key.ReceivedAt(time3), RandomSuffix: "efgh"}
	err = store.schema.Records().ByKey(recordKey2).Put("rec").Do(ctx, store.client)
	assert.NoError(t, err)

	// Run the cleanup
	err = store.Cleanup(ctx, receiver, log.NewNopLogger())
	assert.NoError(t, err)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
file/opened/00001000/github/third/%s
-----
{
  "projectId": 1000,
  "receiverId": "github",
  "exportId": "third",
  "fileId": "%s",
  "state": "opened",
  "mapping": {
    "projectId": 1000,
    "receiverId": "github",
    "exportId": "third",
    "revisionId": 1,
    "tableId": "in.test.test",
    "incremental": false,
    "columns": [
      {
        "type": "id",
        "name": "id"
      }
    ]
  },
  "storageResource": {
    "id": 123,
    "created": "0001-01-01T00:00:00Z",
    "name": "file1.csv",
    "url": "",
    "provider": "",
    "region": "",
    "maxAgeDays": 0
  }
}
>>>>>

<<<<<
record/00001000/github/third/%s_efgh
-----
rec
>>>>>

<<<<<
slice/archived/successful/imported/00001000/github/third/%s/%s
-----
{
  "projectId": 1000,
  "receiverId": "github",
  "exportId": "third",
  "fileId": "%s",
  "sliceId": "%s",
  "state": "archived/successful/imported",
  "mapping": {
    "projectId": 1000,
    "receiverId": "github",
    "exportId": "third",
    "revisionId": 1,
    "tableId": "in.test.test",
    "incremental": false,
    "columns": [
      {
        "type": "id",
        "name": "id"
      }
    ]
  },
  "storageResource": {
    "id": 123,
    "created": "0001-01-01T00:00:00Z",
    "name": "file1.csv",
    "url": "",
    "provider": "",
    "region": "",
    "maxAgeDays": 0
  },
  "sliceNumber": 1
}
>>>>>

<<<<<
task/00001000/github/other.task/2006-01-02T08:04:05.000Z_ghijkl
-----
{
  "projectId": 1000,
  "receiverId": "github",
  "type": "other.task",
  "taskId": "2006-01-02T08:04:05.000Z_ghijkl",
  "createdAt": "2006-01-02T08:04:05.000Z",
  "finishedAt": "%s",
  "workerNode": "node2",
  "lock": "lock2",
  "result": "res"
}
>>>>>
`)
}
