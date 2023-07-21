package cleanup_test

import (
	"context"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/cleanup"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

func TestCleanup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	workerScp, mock := bufferDependencies.NewMockedWorkerScope(t, config.NewWorkerConfig())
	client := mock.TestEtcdClient()
	schema := workerScp.Schema()
	statsRepo := workerScp.StatisticsRepository()
	node := cleanup.NewNode(workerScp, workerScp.Logger().AddPrefix("[cleanup]"))

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
	assert.NoError(t, schema.Configs().Receivers().ByKey(receiver.ReceiverKey).Put(receiver).Do(ctx, client))
	assert.NoError(t, schema.Configs().Exports().ByKey(exportKey1).Put(export1).Do(ctx, client))
	assert.NoError(t, schema.Configs().Exports().ByKey(exportKey2).Put(export2).Do(ctx, client))
	assert.NoError(t, schema.Configs().Exports().ByKey(exportKey3).Put(export3).Do(ctx, client))

	mapping := func(exportKey key.ExportKey) model.Mapping {
		return model.Mapping{
			MappingKey:  key.MappingKey{ExportKey: exportKey, RevisionID: 1},
			TableID:     keboola.TableID{BucketID: keboola.BucketID{Stage: "in", BucketName: "test"}, TableName: "test"},
			Incremental: false,
			Columns:     []column.Column{column.ID{Name: "id", PrimaryKey: false}},
		}
	}

	oldTimeRaw, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05+07:00")
	oldTime := utctime.From(oldTimeRaw)
	timeNow := time.Now()

	// File 1 ----------------------------------------------------------------------------------------------------------
	// State:
	//     An old broken file AFTER EXPIRATION, in Closing state, the slice is Uploaded but not Imported.
	// Expected result:
	//     File, slice, records and statistics are deleted.

	// Add file with a Closing state and created in the past
	fileKey1 := key.FileKey{ExportKey: exportKey1, FileID: key.FileID(oldTime)}
	file1 := model.File{
		FileKey:         fileKey1,
		State:           filestate.Closing,
		Mapping:         mapping(exportKey1),
		StorageResource: &keboola.FileUploadCredentials{File: keboola.File{ID: 123, Name: "file1.csv"}},
	}
	assert.NoError(t, schema.Files().InState(filestate.Closing).ByKey(fileKey1).Put(file1).Do(ctx, client))

	// Add slice for the cleaned-up file
	sliceKey1 := key.SliceKey{FileKey: fileKey1, SliceID: key.SliceID(oldTime)}
	slice1 := model.Slice{
		SliceKey:        sliceKey1,
		Number:          1,
		State:           slicestate.Uploaded,
		Mapping:         mapping(exportKey1),
		StorageResource: &keboola.FileUploadCredentials{File: keboola.File{ID: 123, Name: "file1.csv"}},
	}
	assert.NoError(t, schema.Slices().InState(slicestate.Uploaded).ByKey(sliceKey1).Put(slice1).Do(ctx, client))

	// Add record for the cleaned-up slice
	recordKey1 := key.RecordKey{SliceKey: sliceKey1, ReceivedAt: key.ReceivedAt(oldTime), RandomSuffix: "abcd"}
	assert.NoError(t, schema.Records().ByKey(recordKey1).Put("rec").Do(ctx, client))

	// Add received stats for the cleaned-up slice
	assert.NoError(t, statsRepo.Insert(ctx, "node-123", []statistics.PerAPINode{
		{
			SliceKey: sliceKey1,
			Value: statistics.Value{
				FirstRecordAt: oldTime,
				LastRecordAt:  oldTime,
				RecordsCount:  123,
				RecordsSize:   1 * datasize.KB,
				BodySize:      1 * datasize.KB,
			},
		},
	}))
	moveOp, err := statsRepo.MoveOp(ctx, sliceKey1, statistics.Buffered, statistics.Uploaded, func(value *statistics.Value) {
		*value = value.WithAfterUpload(statistics.AfterUpload{
			RecordsCount: 456,
			FileSize:     1 * datasize.KB,
			FileGZipSize: 500 * datasize.B,
		})
	})
	assert.NoError(t, err)
	assert.NoError(t, moveOp.DoOrErr(ctx, client))

	// File 2 ----------------------------------------------------------------------------------------------------------
	// State:
	//    An file BEFORE EXPIRATION, in Closing state, the slice is Uploaded but not Imported.
	// Expected result:
	//    The file is excluded from the cleanup, nothing is deleted.

	// Add file with a Closing state and created recently
	fileKey2 := key.FileKey{ExportKey: exportKey2, FileID: key.FileID(timeNow)}
	file2 := model.File{
		FileKey:         fileKey2,
		State:           filestate.Closing,
		Mapping:         mapping(exportKey2),
		StorageResource: &keboola.FileUploadCredentials{File: keboola.File{ID: 123, Name: "file1.csv"}},
	}
	assert.NoError(t, schema.Files().InState(filestate.Closing).ByKey(fileKey2).Put(file2).Do(ctx, client))

	// Add slice for the ignored file
	sliceKey2 := key.SliceKey{FileKey: fileKey2, SliceID: key.SliceID(timeNow)}
	slice2 := model.Slice{
		SliceKey:        sliceKey2,
		Number:          1,
		State:           slicestate.Uploaded,
		Mapping:         mapping(exportKey2),
		StorageResource: &keboola.FileUploadCredentials{File: keboola.File{ID: 123, Name: "file1.csv"}},
	}
	assert.NoError(t, schema.Slices().InState(slicestate.Uploaded).ByKey(sliceKey2).Put(slice2).Do(ctx, client))

	// Add record for the ignored slice
	recordKey2 := key.RecordKey{SliceKey: sliceKey2, ReceivedAt: key.ReceivedAt(timeNow), RandomSuffix: "efgh"}
	assert.NoError(t, schema.Records().ByKey(recordKey2).Put("rec").Do(ctx, client))

	// Add received stats for the ignored slice
	assert.NoError(t, statsRepo.Insert(ctx, "node-123", []statistics.PerAPINode{
		{
			SliceKey: sliceKey2,
			Value: statistics.Value{
				FirstRecordAt: utctime.UTCTime(timeNow),
				LastRecordAt:  utctime.UTCTime(timeNow),
				RecordsCount:  456,
				RecordsSize:   2 * datasize.KB,
				BodySize:      2 * datasize.KB,
			},
		},
	}))
	moveOp, err = statsRepo.MoveOp(ctx, sliceKey2, statistics.Buffered, statistics.Uploaded, func(value *statistics.Value) {
		*value = value.WithAfterUpload(statistics.AfterUpload{
			RecordsCount: 456,
			FileSize:     1 * datasize.KB,
			FileGZipSize: 500 * datasize.B,
		})
	})
	assert.NoError(t, err)
	assert.NoError(t, moveOp.DoOrErr(ctx, client))

	// File 3 ----------------------------------------------------------------------------------------------------------
	// State:
	//     An old imported file AFTER EXPIRATION, in Imported state, the slice is Imported.
	// Expected result:
	//     File, slice, records and statistics are deleted,
	//     but statistics are rolled-up and stored to export prefix.

	// Add file with a Closing state and created recently
	fileKey3 := key.FileKey{ExportKey: exportKey3, FileID: key.FileID(oldTime)}
	file3 := model.File{
		FileKey:         fileKey3,
		State:           filestate.Imported,
		Mapping:         mapping(exportKey3),
		StorageResource: &keboola.FileUploadCredentials{File: keboola.File{ID: 123, Name: "file1.csv"}},
	}
	assert.NoError(t, schema.Files().InState(filestate.Imported).ByKey(fileKey3).Put(file3).Do(ctx, client))

	// Add slice for the ignored file
	sliceKey3 := key.SliceKey{FileKey: fileKey3, SliceID: key.SliceID(oldTime)}
	slice3 := model.Slice{
		SliceKey:        sliceKey3,
		Number:          1,
		State:           slicestate.Imported,
		Mapping:         mapping(exportKey3),
		StorageResource: &keboola.FileUploadCredentials{File: keboola.File{ID: 123, Name: "file1.csv"}},
	}
	assert.NoError(t, schema.Slices().InState(slicestate.Imported).ByKey(sliceKey3).Put(slice3).Do(ctx, client))

	// Add record for the ignored slice
	recordKey3 := key.RecordKey{SliceKey: sliceKey3, ReceivedAt: key.ReceivedAt(oldTime), RandomSuffix: "efgh"}
	assert.NoError(t, schema.Records().ByKey(recordKey3).Put("rec").Do(ctx, client))

	// Add received stats for the ignored slice
	assert.NoError(t, statsRepo.Insert(ctx, "node-123", []statistics.PerAPINode{
		{
			SliceKey: sliceKey3,
			Value: statistics.Value{
				FirstRecordAt: oldTime,
				LastRecordAt:  oldTime,
				RecordsCount:  789,
				RecordsSize:   2 * datasize.KB,
				BodySize:      2 * datasize.KB,
			},
		},
	}))
	moveOp, err = statsRepo.MoveOp(ctx, sliceKey3, statistics.Buffered, statistics.Uploaded, func(value *statistics.Value) {
		*value = value.WithAfterUpload(statistics.AfterUpload{
			RecordsCount: 456,
			FileSize:     1 * datasize.KB,
			FileGZipSize: 500 * datasize.B,
		})
	})
	assert.NoError(t, err)
	assert.NoError(t, moveOp.DoOrErr(ctx, client))
	moveOp, err = statsRepo.MoveOp(ctx, sliceKey3, statistics.Uploaded, statistics.Imported, nil)
	assert.NoError(t, err)
	assert.NoError(t, moveOp.DoOrErr(ctx, client))
	// -----------------------------------------------------------------------------------------------------------------

	// Run the cleanup
	assert.NoError(t, node.Check(ctx))

	// Shutdown - wait for tasks
	workerScp.Process().Shutdown(errors.New("bye bye"))
	workerScp.Process().WaitForShutdown()

	// Check logs
	wildcards.Assert(t, `
[task][1000/github/receiver.cleanup/%s]INFO  started task
[task][1000/github/receiver.cleanup/%s]DEBUG  lock acquired "runtime/lock/task/1000/github/receiver.cleanup"
[task][1000/github/receiver.cleanup/%s]DEBUG  deleted slice "1000/github/first/%s"
[task][1000/github/receiver.cleanup/%s]DEBUG  deleted file "1000/github/first/%s"
[task][1000/github/receiver.cleanup/%s]DEBUG  deleted slice "1000/github/third/%s"
[task][1000/github/receiver.cleanup/%s]DEBUG  deleted file "1000/github/third/%s"
[task][1000/github/receiver.cleanup/%s]INFO  deleted "2" files, "2" slices, "2" records
[task][1000/github/receiver.cleanup/%s]INFO  task succeeded (%s): receiver "1000/github" has been cleaned
[task][1000/github/receiver.cleanup/%s]DEBUG  lock released "runtime/lock/task/1000/github/receiver.cleanup"
`, strhelper.FilterLines(`^\[task\]\[1000/`, mock.DebugLogger().AllMessages()))

	// Check keys
	etcdhelper.AssertKVsString(t, client, `
<<<<<
config/receiver/1000/github
-----
%A
>>>>>

<<<<<
config/export/1000/github/first
-----
%A
>>>>>

<<<<<
config/export/1000/github/second
-----
%A
>>>>>

<<<<<
config/export/1000/github/third
-----
%A
>>>>>

<<<<<
file/closing/1000/github/second/%s
-----
{
  "projectId": 1000,
  "receiverId": "github",
  "exportId": "second",
  "fileId": "%s",
  "state": "closing",
  "mapping": %A,
  "storageResource": %A
}
>>>>>

<<<<<
record/1000/github/second/%s_efgh
-----
rec
>>>>>

<<<<<
slice/active/closed/uploaded/1000/github/second/%s/%s
-----
{
  "projectId": 1000,
  "receiverId": "github",
  "exportId": "second",
  "fileId": "%s",
  "sliceId": "%s",
  "state": "active/closed/uploaded",
  "mapping": %A,
  "storageResource": %A,
  "sliceNumber": 1
}
>>>>>

<<<<<
stats/uploaded/1000/github/second/%s/%s/_nodes_sum
-----
{
  "firstRecordAt": "%s",
  "lastRecordAt": "%s",
  "recordsCount": 456,
  "recordsSize": "2KB",
  "bodySize": "2KB",
  "fileSize": "1KB",
  "fileGZipSize": "500B"
}
>>>>>

<<<<<
stats/imported/1000/github/third/_cleanup_sum
-----
{
  "firstRecordAt": "%s",
  "lastRecordAt": "%s",
  "recordsCount": 456,
  "recordsSize": "2KB",
  "bodySize": "2KB",
  "fileSize": "1KB",
  "fileGZipSize": "500B"
}
>>>>>

<<<<<
task/1000/github/receiver.cleanup/%s
-----
{
  "projectId": 1000,
  "taskId": "github/receiver.cleanup/%s",
  "type": "receiver.cleanup",
  "createdAt": "%s",
  "finishedAt": "%s",
  "node": "%s",
  "lock": "runtime/lock/task/1000/github/receiver.cleanup",
  "result": "receiver \"1000/github\" has been cleaned",
  "duration": %d
}
>>>>>
`)
}
