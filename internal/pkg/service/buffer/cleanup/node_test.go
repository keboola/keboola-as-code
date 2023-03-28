package cleanup_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/cleanup"
	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestCleanup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	etcdNamespace := "unit-" + t.Name() + "-" + idgenerator.Random(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	d := bufferDependencies.NewMockedDeps(t, dependencies.WithEtcdNamespace(etcdNamespace))
	schema := d.Schema()

	node := cleanup.NewNode(d, d.Logger().AddPrefix("[cleanup]"))

	// Create receiver and 3 exports
	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "github"}
	exportKey1 := key.ExportKey{ExportID: "first", ReceiverKey: receiverKey}
	exportKey2 := key.ExportKey{ExportID: "second", ReceiverKey: receiverKey}
	exportKey3 := key.ExportKey{ExportID: "third", ReceiverKey: receiverKey}
	receiver := model.ReceiverBase{
		ReceiverKey: receiverKey,
		Name:        "rec1",
		Secret:      "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
	}
	export1 := model.ExportBase{
		ExportKey:        exportKey1,
		Name:             "First Export",
		ImportConditions: model.DefaultImportConditions(),
	}
	export2 := model.ExportBase{
		ExportKey:        exportKey2,
		Name:             "Second Export",
		ImportConditions: model.DefaultImportConditions(),
	}
	export3 := model.ExportBase{
		ExportKey:        exportKey3,
		Name:             "Third Export",
		ImportConditions: model.DefaultImportConditions(),
	}
	assert.NoError(t, schema.Configs().Receivers().ByKey(receiver.ReceiverKey).Put(receiver).DoOrErr(ctx, client))
	assert.NoError(t, schema.Configs().Exports().ByKey(exportKey1).Put(export1).DoOrErr(ctx, client))
	assert.NoError(t, schema.Configs().Exports().ByKey(exportKey2).Put(export2).DoOrErr(ctx, client))
	assert.NoError(t, schema.Configs().Exports().ByKey(exportKey3).Put(export3).DoOrErr(ctx, client))

	// Add task without a finishedAt timestamp but too old - will be deleted
	createdAtRaw, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05+07:00")
	createdAt := model.UTCTime(createdAtRaw)
	taskKey1 := key.TaskKey{ProjectID: receiverKey.ProjectID, TaskID: key.TaskID(fmt.Sprintf("%s/%s/%s_%s", receiverKey.ReceiverID.String(), "some.task", createdAt.String(), "abcdef"))}
	task1 := model.Task{
		TaskKey:    taskKey1,
		Type:       "some.task",
		CreatedAt:  createdAt,
		FinishedAt: nil,
		Node:       "node1",
		Lock:       "lock1",
		Result:     "",
		Error:      "err",
		Duration:   nil,
	}
	assert.NoError(t, schema.Tasks().ByKey(taskKey1).Put(task1).DoOrErr(ctx, client))

	// Add task with a finishedAt timestamp in the past - will be deleted
	time2, _ := time.Parse(time.RFC3339, "2008-01-02T15:04:05+07:00")
	time2Key := key.UTCTime(time2)
	taskKey2 := key.TaskKey{ProjectID: receiverKey.ProjectID, TaskID: key.TaskID(fmt.Sprintf("%s/%s/%s_%s", receiverKey.ReceiverID.String(), "other.task", createdAt.String(), "ghijkl"))}
	task2 := model.Task{
		TaskKey:    taskKey2,
		Type:       "other.task",
		CreatedAt:  createdAt,
		FinishedAt: &time2Key,
		Node:       "node2",
		Lock:       "lock2",
		Result:     "res",
		Error:      "",
		Duration:   nil,
	}
	assert.NoError(t, schema.Tasks().ByKey(taskKey2).Put(task2).DoOrErr(ctx, client))

	// Add task with a finishedAt timestamp before a moment - will be ignored
	time3 := time.Now()
	time3Key := key.UTCTime(time3)
	taskKey3 := key.TaskKey{ProjectID: receiverKey.ProjectID, TaskID: key.TaskID(fmt.Sprintf("%s/%s/%s_%s", receiverKey.ReceiverID.String(), "other.task", createdAt.String(), "ghijkl"))}
	task3 := model.Task{
		TaskKey:    taskKey3,
		Type:       "other.task",
		CreatedAt:  createdAt,
		FinishedAt: &time3Key,
		Node:       "node2",
		Lock:       "lock2",
		Result:     "res",
		Error:      "",
		Duration:   nil,
	}
	assert.NoError(t, schema.Tasks().ByKey(taskKey3).Put(task3).DoOrErr(ctx, client))

	// Add file with an Opened state and created in the past - will be deleted
	fileKey1 := key.FileKey{ExportKey: exportKey1, FileID: key.FileID(createdAt)}
	file1 := model.File{
		FileKey: fileKey1,
		State:   filestate.Opened,
		Mapping: model.Mapping{
			MappingKey:  key.MappingKey{ExportKey: exportKey1, RevisionID: 1},
			TableID:     keboola.TableID{BucketID: keboola.BucketID{Stage: "in", BucketName: "test"}, TableName: "test"},
			Incremental: false,
			Columns:     []column.Column{column.ID{Name: "id", PrimaryKey: false}},
		},
		StorageResource: &keboola.FileUploadCredentials{File: keboola.File{ID: 123, Name: "file1.csv"}},
	}
	assert.NoError(t, schema.Files().InState(filestate.Opened).ByKey(fileKey1).Put(file1).DoOrErr(ctx, client))

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
		StorageResource: &keboola.FileUploadCredentials{File: keboola.File{ID: 123, Name: "file1.csv"}},
	}
	assert.NoError(t, schema.Files().InState(filestate.Opened).ByKey(fileKey2).Put(file2).DoOrErr(ctx, client))

	// Add slice for the cleaned-up file - will be deleted
	sliceKey1 := key.SliceKey{FileKey: fileKey1, SliceID: key.SliceID(createdAt)}
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
		StorageResource: &keboola.FileUploadCredentials{File: keboola.File{ID: 123, Name: "file1.csv"}},
	}
	assert.NoError(t, schema.Slices().InState(slicestate.Imported).ByKey(sliceKey1).Put(slice1).DoOrErr(ctx, client))

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
		StorageResource: &keboola.FileUploadCredentials{File: keboola.File{ID: 123, Name: "file1.csv"}},
	}
	assert.NoError(t, schema.Slices().InState(slicestate.Imported).ByKey(sliceKey2).Put(slice2).DoOrErr(ctx, client))

	// Add record for the cleaned-up slice - will be deleted
	recordKey1 := key.RecordKey{SliceKey: sliceKey1, ReceivedAt: key.ReceivedAt(createdAt), RandomSuffix: "abcd"}
	assert.NoError(t, schema.Records().ByKey(recordKey1).Put("rec").DoOrErr(ctx, client))

	// Add record for the ignored slice - will be ignored
	recordKey2 := key.RecordKey{SliceKey: sliceKey2, ReceivedAt: key.ReceivedAt(time3), RandomSuffix: "efgh"}
	assert.NoError(t, schema.Records().ByKey(recordKey2).Put("rec").DoOrErr(ctx, client))

	// Add received stats for the cleaned-up slice - will be deleted
	assert.NoError(t, schema.ReceivedStats().InSlice(sliceKey1).ByNodeID("node-123").Put(model.SliceStats{
		SliceNodeKey: key.SliceNodeKey{SliceKey: sliceKey1, NodeID: "node-123"},
		Stats: model.Stats{
			LastRecordAt: model.UTCTime(time3),
			RecordsCount: 123,
			RecordsSize:  1 * datasize.KB,
			BodySize:     1 * datasize.KB,
		},
	}).DoOrErr(ctx, client))

	// Add received stats for the ignored slice - will be ignored
	assert.NoError(t, schema.ReceivedStats().InSlice(sliceKey2).ByNodeID("node-123").Put(model.SliceStats{
		SliceNodeKey: key.SliceNodeKey{SliceKey: sliceKey2, NodeID: "node-123"},
		Stats: model.Stats{
			LastRecordAt: model.UTCTime(time3),
			RecordsCount: 456,
			RecordsSize:  2 * datasize.KB,
			BodySize:     2 * datasize.KB,
		},
	}).DoOrErr(ctx, client))

	// Run the cleanup
	assert.NoError(t, node.Check(ctx))

	// Shutdown - wait for tasks
	d.Process().Shutdown(errors.New("bye bye"))
	d.Process().WaitForShutdown()

	// Check logs
	wildcards.Assert(t, `
%A
[task][00001000/github/receiver.cleanup/%s]INFO  started task
[task][00001000/github/receiver.cleanup/%s]DEBUG  lock acquired "runtime/lock/task/00001000/github/receiver.cleanup"
[cleanup]INFO  started "1" receiver cleanup tasks
%A
[task][00001000/github/receiver.cleanup/%s]DEBUG  deleted task "00001000/github/some.task/2006-01-02T08:04:05.000Z_abcdef"
[task][00001000/github/receiver.cleanup/%s]INFO  deleted "1" tasks
[task][00001000/github/receiver.cleanup/%s]DEBUG  deleted slice "00001000/github/first/2006-01-02T08:04:05.000Z"
[task][00001000/github/receiver.cleanup/%s]DEBUG  deleted file "00001000/github/first/2006-01-02T08:04:05.000Z"
[task][00001000/github/receiver.cleanup/%s]INFO  deleted "1" files, "1" slices, "1" records
[task][00001000/github/receiver.cleanup/%s]INFO  task succeeded (%s): receiver "00001000/github" has been cleaned
[task][00001000/github/receiver.cleanup/%s]DEBUG  lock released "runtime/lock/task/00001000/github/receiver.cleanup"
%A
`, d.DebugLogger().AllMessages())

	// Check keys
	etcdhelper.AssertKVsString(t, client, `
<<<<<
config/receiver/00001000/github
-----
%A
>>>>>

<<<<<
config/export/00001000/github/first
-----
%A
>>>>>

<<<<<
config/export/00001000/github/second
-----
%A
>>>>>

<<<<<
config/export/00001000/github/third
-----
%A
>>>>>

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
stats/received/00001000/github/third/%s/%s/node-123
-----
 {
  "projectId": 1000,
  "receiverId": "github",
  "exportId": "third",
  "fileId": "%s",
  "sliceId": "%s",
  "nodeId": "node-123",
  "lastRecordAt": "%s",
  "recordsCount": 456,
  "recordsSize": "2KB",
  "bodySize": "2KB"
}
>>>>>

<<<<<
task/00001000/github/other.task/2006-01-02T08:04:05.000Z_ghijkl
-----
{
  "projectId": 1000,
  "taskId": "github/other.task/2006-01-02T08:04:05.000Z_ghijkl",
  "type": "other.task",
  "createdAt": "2006-01-02T08:04:05.000Z",
  "finishedAt": "%s",
  "node": "node2",
  "lock": "lock2",
  "result": "res"
}
>>>>>

<<<<<
task/00001000/github/receiver.cleanup/%s
-----
{
  "projectId": 1000,
  "taskId": "github/receiver.cleanup/%s",
  "type": "receiver.cleanup",
  "createdAt": "%s",
  "finishedAt": "%s",
  "node": "%s",
  "lock": "runtime/lock/task/00001000/github/receiver.cleanup",
  "result": "receiver \"00001000/github\" has been cleaned",
  "duration": %d
}
>>>>>
`)
}
