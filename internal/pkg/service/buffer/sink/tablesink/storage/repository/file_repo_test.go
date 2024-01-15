package repository_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/volume/assignment"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestFileRepository_Operations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey1 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}
	sinkKey2 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-2"}
	nonExistentFileKey := storage.FileKey{
		SinkKey: sinkKey1,
		FileID:  storage.FileID{OpenedAt: utctime.MustParse("2000-01-01T18:00:00.000Z")},
	}

	// Get services
	d, mocked := dependencies.NewMockedTableSinkScope(t, config.New(), commonDeps.WithClock(clk))
	rb := rollback.New(d.Logger())
	client := mocked.TestEtcdClient()
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	fileRepo := storageRepo.File()
	sliceRepo := storageRepo.Slice()
	tokenRepo := storageRepo.Token()
	volumeRepo := storageRepo.Volume()

	// Mock file API calls
	transport := mocked.MockedHTTPTransport()
	mockStorageAPICalls(t, clk, branchKey, transport)

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		registerWriterVolumes(t, ctx, volumeRepo, session, 5)
	}

	// Empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		// List - empty
		files, err := fileRepo.List(projectID).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, files)
		files, err = fileRepo.List(sinkKey1).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, files)
	}
	{
		// Get - not found
		if err := fileRepo.Get(nonExistentFileKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `file "123/456/my-source/my-sink-1/2000-01-01T18:00:00.000Z" not found in the sink`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create (the first Rotate) - parent sink doesn't exists
	// -----------------------------------------------------------------------------------------------------------------
	// Entity exists only in memory
	{
		if err := fileRepo.Rotate(rb, clk.Now(), sinkKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "123/456/my-source/my-sink-1" not found in the source`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create parent branch, source, sinks and tokens
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch).Do(ctx).Err())

		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create("Create source", &source).Do(ctx).Err())

		sink1 := test.NewSink(sinkKey1)
		sink1.Table.Storage = sinkStorageConfig(3, []string{"hdd"})
		require.NoError(t, defRepo.Sink().Create("Create sink", &sink1).Do(ctx).Err())

		sink2 := test.NewSink(sinkKey2)
		sink2.Table.Storage = sinkStorageConfig(3, []string{"ssd"})
		require.NoError(t, defRepo.Sink().Create("Create sink", &sink2).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink1.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink2.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
	}

	// Create (the first Rotate)
	// See TestFileRepository_Rotate for more rotation tests.
	// -----------------------------------------------------------------------------------------------------------------
	var fileKey1, fileKey2 storage.FileKey
	{
		// Create 2 files in different sinks
		clk.Add(time.Hour)
		file1, err := fileRepo.Rotate(rb, clk.Now(), sinkKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.NotNil(t, file1.StagingStorage.UploadCredentials)
		fileKey1 = file1.FileKey

		clk.Add(time.Hour)
		file2, err := fileRepo.Rotate(rb, clk.Now(), sinkKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.NotNil(t, file2.StagingStorage.UploadCredentials)
		fileKey2 = file2.FileKey
	}
	{
		// List
		files, err := fileRepo.List(projectID).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, files, 2)
		files, err = fileRepo.List(branchKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, files, 2)
		files, err = fileRepo.List(sourceKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, files, 2)
		files, err = fileRepo.List(sinkKey1).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, files, 1)
		files, err = fileRepo.List(sinkKey2).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, files, 1)
	}
	{
		// Get
		result1, err := fileRepo.Get(fileKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, "2000-01-01T02:00:00.000Z", result1.OpenedAt().String())
		result2, err := fileRepo.Get(fileKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, "2000-01-01T03:00:00.000Z", result2.OpenedAt().String())
	}

	// File rotation has created slices in assigned volumes
	// -----------------------------------------------------------------------------------------------------------------
	var sliceKeys1, sliceKeys2 []storage.SliceKey
	{
		// Slices in file1
		sliceID1 := storage.SliceID{OpenedAt: fileKey1.OpenedAt()}
		require.NoError(t, sliceRepo.List(fileKey1).Do(ctx).ForEachValue(
			func(value storage.Slice, header *iterator.Header) error {
				sliceKeys1 = append(sliceKeys1, value.SliceKey)
				return nil
			},
		))
		assert.Equal(t, []storage.SliceKey{
			{SliceID: sliceID1, FileVolumeKey: storage.FileVolumeKey{FileKey: fileKey1, VolumeID: "my-volume-1"}}, // hdd
			{SliceID: sliceID1, FileVolumeKey: storage.FileVolumeKey{FileKey: fileKey1, VolumeID: "my-volume-3"}}, // hdd
			{SliceID: sliceID1, FileVolumeKey: storage.FileVolumeKey{FileKey: fileKey1, VolumeID: "my-volume-5"}}, // hdd
		}, sliceKeys1)

		// Slices in file2
		sliceID2 := storage.SliceID{OpenedAt: fileKey2.OpenedAt()}
		require.NoError(t, sliceRepo.List(fileKey2).Do(ctx).ForEachValue(
			func(value storage.Slice, header *iterator.Header) error {
				sliceKeys2 = append(sliceKeys2, value.SliceKey)
				return nil
			},
		))
		assert.Equal(t, []storage.SliceKey{
			{SliceID: sliceID2, FileVolumeKey: storage.FileVolumeKey{FileKey: fileKey2, VolumeID: "my-volume-2"}}, // ssd
			{SliceID: sliceID2, FileVolumeKey: storage.FileVolumeKey{FileKey: fileKey2, VolumeID: "my-volume-4"}}, // ssd
			{SliceID: sliceID2, FileVolumeKey: storage.FileVolumeKey{FileKey: fileKey2, VolumeID: "my-volume-5"}}, // hdd
		}, sliceKeys2)
	}

	// Rotate - already exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := fileRepo.Rotate(rb, fileKey1.OpenedAt().Time(), sinkKey1).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `file "123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z" already exists in the sink`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusConflict, err)
		}
	}

	// Increment retry attempt - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		err := fileRepo.IncrementRetry(clk.Now(), nonExistentFileKey, "some error").Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `file "123/456/my-source/my-sink-1/2000-01-01T18:00:00.000Z" not found in the sink`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Increment retry attempt
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := fileRepo.IncrementRetry(clk.Now(), fileKey1, "some error").Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, 1, result.RetryAttempt)
		assert.Equal(t, "some error", result.RetryReason)
		assert.Equal(t, "2000-01-01T04:00:00.000Z", result.LastFailedAt.String())
		assert.Equal(t, "2000-01-01T04:02:00.000Z", result.RetryAfter.String())

		file1, err := fileRepo.Get(fileKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, result, file1)
	}

	// Switch file state to storage.FileClosing
	// -----------------------------------------------------------------------------------------------------------------
	switchFileStates(t, ctx, clk, fileRepo, fileKey1, []storage.FileState{
		storage.FileWriting, storage.FileClosing,
	})

	// Switch file state - slices are not uploaded
	// -----------------------------------------------------------------------------------------------------------------
	if err := fileRepo.StateTransition(clk.Now(), fileKey1, storage.FileClosing, storage.FileImporting).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`
unexpected slice "123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-1/2000-01-01T02:00:00.000Z" state:
- unexpected combination: file state "importing" and slice state "closing"
`), err.Error())
		serviceError.AssertErrorStatusCode(t, http.StatusBadRequest, err)
	}

	// Simulate slices upload to unblock the storage.FileImporting state
	// -----------------------------------------------------------------------------------------------------------------
	{
		for _, sliceKey := range sliceKeys1 {
			switchSliceStates(t, ctx, clk, sliceRepo, sliceKey, []storage.SliceState{
				storage.SliceClosing, storage.SliceUploading, storage.SliceUploaded,
			})
		}
	}

	// Switch file state to storage.FileImported
	// -----------------------------------------------------------------------------------------------------------------
	switchFileStates(t, ctx, clk, fileRepo, fileKey1, []storage.FileState{
		storage.FileClosing, storage.FileImporting, storage.FileImported,
	})

	// Switch file state - already in the state
	// -----------------------------------------------------------------------------------------------------------------
	if err := fileRepo.StateTransition(clk.Now(), fileKey1, storage.FileImported, storage.FileImported).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `unexpected file "123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z" state transition from "imported" to "imported"`, err.Error())
		serviceError.AssertErrorStatusCode(t, http.StatusBadRequest, err)
	}

	// Switch file state - unexpected transition
	// -----------------------------------------------------------------------------------------------------------------
	if err := fileRepo.StateTransition(clk.Now(), fileKey1, storage.FileImported, storage.FileImporting).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `unexpected file "123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z" state transition from "imported" to "importing"`, err.Error())
		serviceError.AssertErrorStatusCode(t, http.StatusBadRequest, err)
	}

	// Delete
	// -----------------------------------------------------------------------------------------------------------------
	{
		assert.NoError(t, fileRepo.Delete(fileKey2).Do(ctx).Err())
	}
	{
		// Get - not found
		if err := fileRepo.Get(fileKey2).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `file "123/456/my-source/my-sink-2/2000-01-01T03:00:00.000Z" not found in the sink`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// List - empty
		files, err := fileRepo.List(sinkKey2).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, files)
	}

	// Delete - not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := fileRepo.Delete(nonExistentFileKey).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `file "123/456/my-source/my-sink-1/2000-01-01T18:00:00.000Z" not found in the sink`, err.Error())
		serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
	}

	// Check etcd state - file2 has been deleted, but file 1 exists
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/file/all/123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T02:00:00.000Z",
  "type": "csv",
  "state": "imported",
  "closingAt": "2000-01-01T05:00:00.000Z",
  "importingAt": "2000-01-01T12:00:00.000Z",
  "importedAt": "2000-01-01T13:00:00.000Z",
  "columns": [
    {
      "type": "datetime",
      "name": "datetime"
    },
    {
      "type": "body",
      "name": "body"
    }
  ],
  "assignment": {
    "config": {
      "count": 3,
      "preferredTypes": [
        "hdd"
      ]
    },
    "volumes": [
      "my-volume-5",
      "my-volume-3",
      "my-volume-1"
    ]
  },
  "local": {
    "dir": "123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z",
    "compression": {
      "type": "gzip",
      "gzip": {
        "level": 1,
        "implementation": "parallel",
        "blockSize": "256KB",
        "concurrency": 0
      }
    },
    "diskSync": {
      "mode": "disk",
      "wait": true,
      "checkInterval": 5000000,
      "countTrigger": 500,
      "bytesTrigger": "1MB",
      "intervalTrigger": 50000000
    },
    "diskAllocation": {
      "enabled": true,
      "size": "100MB",
      "sizePercent": 110
    }
  },
  "staging": {
    "compression": {
      "type": "gzip",
      "gzip": {
        "level": 1,
        "implementation": "parallel",
        "blockSize": "256KB",
        "concurrency": 0
      }
    },
    "credentials": {%A},
    "credentialsExpiration": "2000-01-01T03:00:00.000Z"
  },
  "target": {
    "tableId": "in.bucket.table"
  }
}
>>>>>

<<<<<
storage/file/level/target/123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T02:00:00.000Z",
%A
}
>>>>>

<<<<<
storage/slice/level/target/123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-1/2000-01-01T02:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T02:00:00.000Z",
  "volumeId": "my-volume-1",
  "sliceOpenedAt": "2000-01-01T02:00:00.000Z",
  "type": "csv",
  "state": "imported",
  %A
}
>>>>>

<<<<<
storage/slice/level/target/123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-3/2000-01-01T02:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T02:00:00.000Z",
  "volumeId": "my-volume-3",
  "sliceOpenedAt": "2000-01-01T02:00:00.000Z",
  "type": "csv",
  "state": "imported",
  %A
}
>>>>>

<<<<<
storage/slice/level/target/123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-5/2000-01-01T02:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T02:00:00.000Z",
  "volumeId": "my-volume-5",
  "sliceOpenedAt": "2000-01-01T02:00:00.000Z",
  "type": "csv",
  "state": "imported",
  %A
}
>>>>>
`, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/slice/all|storage/secret/token/|storage/volume"))
}

func TestFileRepository_Rotate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}

	// Get services
	d, mocked := dependencies.NewMockedTableSinkScope(t, config.New(), commonDeps.WithClock(clk))
	client := mocked.TestEtcdClient()
	rb := rollback.New(d.Logger())
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	fileFacade := storageRepo.File()
	tokenRepo := storageRepo.Token()
	volumeRepo := storageRepo.Volume()

	// Mock file API calls
	transport := mocked.MockedHTTPTransport()
	mockStorageAPICalls(t, clk, branchKey, transport)

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		registerWriterVolumes(t, ctx, volumeRepo, session, 2)
	}

	// Create parent branch, source, sink and token
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create("Create source", &source).Do(ctx).Err())
		sink := test.NewSink(sinkKey)
		sink.Table.Storage = sinkStorageConfig(2, []string{"default"})
		require.NoError(t, defRepo.Sink().Create("Create sink", &sink).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
	}

	// Create (the first file Rotate operation)
	// -----------------------------------------------------------------------------------------------------------------
	var file1 storage.File
	{
		var err error
		clk.Add(time.Hour)
		file1, err = fileFacade.Rotate(rb, clk.Now(), sinkKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), file1.OpenedAt().Time())
	}

	// Rotate file (2)
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		file2, err := fileFacade.Rotate(rb, clk.Now(), sinkKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), file2.OpenedAt().Time())
	}

	// Rotate file (3)
	// -----------------------------------------------------------------------------------------------------------------
	var file3 storage.File
	{
		var err error
		clk.Add(time.Hour)
		file3, err = fileFacade.Rotate(rb, clk.Now(), sinkKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), file3.OpenedAt().Time())
	}

	// Check Storage API calls
	// -----------------------------------------------------------------------------------------------------------------
	// File prepare endpoint should be called N times
	assert.Equal(t, 3, transport.GetCallCountInfo()["POST /v2/storage/branch/456/files/prepare"])
	assert.Equal(t, 0, transport.GetCallCountInfo()[`DELETE =~/v2/storage/branch/456/files/\d+$`])

	// Test rollback, delete file endpoint should be called N times
	rb.Invoke(ctx)
	assert.Equal(t, 3, transport.GetCallCountInfo()[`DELETE =~/v2/storage/branch/456/files/\d+$`])

	// Check etcd state
	//   - Only the last file is in the storage.FileWriting state.
	//   - Other files are in the storage.FileClosing state.
	//   - Slices are switched to the storage.SliceClosing state together with the file state transition.
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/file/level/local/123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T02:00:00.000Z",
  "type": "csv",
  "state": "closing",
  "closingAt": "2000-01-01T03:00:00.000Z",
  %A
  "staging": {
    %A
    "credentials": {
      "id": 1%d%d%d,
      %A
    },
    %A
  },
  %A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source/my-sink-1/2000-01-01T03:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T03:00:00.000Z",
  "type": "csv",
  "state": "closing",
  "closingAt": "2000-01-01T04:00:00.000Z",
  %A
  "staging": {
    %A
    "credentials": {
      "id": 1%d%d%d,
      %A
    },
    %A
  },
  %A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source/my-sink-1/2000-01-01T04:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T04:00:00.000Z",
  "type": "csv",
  "state": "writing",
  %A
  "staging": {
    %A
    "credentials": {
      "id": 1%d%d%d,
      %A
    },
    %A
  },
  %A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-1/2000-01-01T02:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T02:00:00.000Z",
  "volumeId": "my-volume-1",
  "sliceOpenedAt": "2000-01-01T02:00:00.000Z",
  "type": "csv",
  "state": "closing",
  "closingAt": "2000-01-01T03:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-2/2000-01-01T02:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T02:00:00.000Z",
  "volumeId": "my-volume-2",
  "sliceOpenedAt": "2000-01-01T02:00:00.000Z",
  "type": "csv",
  "state": "closing",
  "closingAt": "2000-01-01T03:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-1/2000-01-01T03:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T03:00:00.000Z",
  "volumeId": "my-volume-1",
  "sliceOpenedAt": "2000-01-01T03:00:00.000Z",
  "type": "csv",
  "state": "closing",
  "closingAt": "2000-01-01T04:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T03:00:00.000Z",
  "volumeId": "my-volume-2",
  "sliceOpenedAt": "2000-01-01T03:00:00.000Z",
  "type": "csv",
  "state": "closing",
  "closingAt": "2000-01-01T04:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T04:00:00.000Z/my-volume-1/2000-01-01T04:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T04:00:00.000Z",
  "volumeId": "my-volume-1",
  "sliceOpenedAt": "2000-01-01T04:00:00.000Z",
  "type": "csv",
  "state": "writing",
  %A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T04:00:00.000Z/my-volume-2/2000-01-01T04:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T04:00:00.000Z",
  "volumeId": "my-volume-2",
  "sliceOpenedAt": "2000-01-01T04:00:00.000Z",
  "type": "csv",
  "state": "writing",
  %A
}
>>>>>
`, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/all/|storage/slice/all/|storage/secret/token/|storage/volume/"))
}

func TestFileRepository_RotateOnSinkMod(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T02:00:00.000Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}

	// Get services
	d, mocked := dependencies.NewMockedTableSinkScope(t, config.New(), commonDeps.WithClock(clk))
	client := mocked.TestEtcdClient()
	rb := rollback.New(d.Logger())
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	fileFacade := storageRepo.File()
	tokenRepo := storageRepo.Token()
	volumeRepo := storageRepo.Volume()

	// Mock file API calls
	transport := mocked.MockedHTTPTransport()
	mockStorageAPICalls(t, clk, branchKey, transport)

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		registerWriterVolumes(t, ctx, volumeRepo, session, 1)
	}

	// Create parent branch, source and sink
	// -----------------------------------------------------------------------------------------------------------------
	var sink definition.Sink
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create("Create source", &source).Do(ctx).Err())
		sink = test.NewSink(sinkKey)
		require.NoError(t, defRepo.Sink().Create("Create sink", &sink).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
	}

	// Create (the first file Rotate)
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		sink.Table.Mapping.Columns = column.Columns{column.Body{Name: "body1"}}
		file1, err := fileFacade.RotateOnSinkMod(rb, clk.Now(), sink).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), file1.OpenedAt().Time())
	}

	// Rotate file (2)
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		sink.Table.Mapping.Columns = column.Columns{column.Body{Name: "body2"}}
		file2, err := fileFacade.RotateOnSinkMod(rb, clk.Now(), sink).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), file2.OpenedAt().Time())
	}

	// Rotate file (3)
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		sink.Table.Mapping.Columns = column.Columns{column.Body{Name: "body3"}}
		file3, err := fileFacade.RotateOnSinkMod(rb, clk.Now(), sink).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), file3.OpenedAt().Time())
	}

	// Check Storage API calls
	// -----------------------------------------------------------------------------------------------------------------
	// File prepare endpoint should be called N times
	assert.Equal(t, 3, transport.GetCallCountInfo()["POST /v2/storage/branch/456/files/prepare"])
	assert.Equal(t, 0, transport.GetCallCountInfo()[`DELETE =~/v2/storage/branch/456/files/\d+$`])

	// Test rollback, delete file endpoint should be called N times
	rb.Invoke(ctx)
	assert.Equal(t, 3, transport.GetCallCountInfo()[`DELETE =~/v2/storage/branch/456/files/\d+$`])

	// Check etcd state
	//   - Only the last file is in the storage.FileWriting state.
	//   - Other files are in the storage.FileClosing state.
	//   - Columns definition is copied from the provided Sink entity.
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/file/level/local/123/456/my-source/my-sink-1/2000-01-01T03:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T03:00:00.000Z",
  "type": "csv",
  "state": "closing",
  "closingAt": "2000-01-01T04:00:00.000Z",
  "columns": [
    {
      "type": "body",
      "name": "body1"
    }
  ],
  %A
  "staging": {
    %A
    "credentials": {
      "id": 1%d%d%d,
      %A
    },
    %A
  },
  %A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source/my-sink-1/2000-01-01T04:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T04:00:00.000Z",
  "type": "csv",
  "state": "closing",
  "closingAt": "2000-01-01T05:00:00.000Z",
  "columns": [
    {
      "type": "body",
      "name": "body2"
    }
  ],
  %A
  "staging": {
    %A
    "credentials": {
      "id": 1%d%d%d,
      %A
    },
    %A
  },
  %A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source/my-sink-1/2000-01-01T05:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T05:00:00.000Z",
  "type": "csv",
  "state": "writing",
  "columns": [
    {
      "type": "body",
      "name": "body3"
    }
  ],
  %A
  "staging": {
    %A
    "credentials": {
      "id": 1%d%d%d,
      %A
    },
    %A
  },
  %A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T03:00:00.000Z/my-volume-1/2000-01-01T03:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T03:00:00.000Z",
  "volumeId": "my-volume-1",
  "sliceOpenedAt": "2000-01-01T03:00:00.000Z",
  "type": "csv",
  "state": "closing",
  "closingAt": "2000-01-01T04:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T04:00:00.000Z/my-volume-1/2000-01-01T04:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T04:00:00.000Z",
  "volumeId": "my-volume-1",
  "sliceOpenedAt": "2000-01-01T04:00:00.000Z",
  "type": "csv",
  "state": "closing",
  "closingAt": "2000-01-01T05:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T05:00:00.000Z/my-volume-1/2000-01-01T05:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T05:00:00.000Z",
  "volumeId": "my-volume-1",
  "sliceOpenedAt": "2000-01-01T05:00:00.000Z",
  "type": "csv",
  "state": "writing",
  %A
}
>>>>>
`, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/all/|storage/slice/all|storage/secret/token/|storage/volume/writer/my-volume-1"))
}

func TestFileRepository_CloseAllIn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}

	// Get services
	d, mocked := dependencies.NewMockedTableSinkScope(t, config.New(), commonDeps.WithClock(clk))
	rb := rollback.New(d.Logger())
	client := mocked.TestEtcdClient()
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	fileRepo := storageRepo.File()
	tokenRepo := storageRepo.Token()
	volumeRepo := storageRepo.Volume()

	// Mock file API calls
	transport := mocked.MockedHTTPTransport()
	mockStorageAPICalls(t, clk, branchKey, transport)

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		registerWriterVolumes(t, ctx, volumeRepo, session, 1)
	}

	// Create sink
	branch := test.NewBranch(branchKey)
	require.NoError(t, defRepo.Branch().Create(&branch).Do(ctx).Err())
	source := test.NewSource(sourceKey)
	require.NoError(t, defRepo.Source().Create("Create source", &source).Do(ctx).Err())
	sink := test.NewSink(sinkKey)
	require.NoError(t, defRepo.Sink().Create("Create sink", &sink).Do(ctx).Err())
	require.NoError(t, tokenRepo.Put(sink.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())

	// Create 2 files, with 2 slices
	require.NoError(t, fileRepo.Rotate(rb, clk.Now(), sinkKey).Do(ctx).Err())
	clk.Add(time.Hour)
	require.NoError(t, fileRepo.Rotate(rb, clk.Now(), sinkKey).Do(ctx).Err())

	// Close the last file
	clk.Add(time.Hour)
	require.NoError(t, fileRepo.CloseAllIn(clk.Now(), sinkKey).Do(ctx).Err())

	// Check etcd state
	expectedEtcdState := `
<<<<<
storage/file/level/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z
-----
{
  %A
  "fileOpenedAt": "2000-01-01T01:00:00.000Z",
  %A
  "state": "closing",
  "closingAt": "2000-01-01T02:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z
-----
{
  %A
  "fileOpenedAt": "2000-01-01T02:00:00.000Z",
  %A
  "state": "closing",
  "closingAt": "2000-01-01T03:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T01:00:00.000Z",
  "volumeId": "my-volume-1",
  "sliceOpenedAt": "2000-01-01T01:00:00.000Z",
  "type": "csv",
  "state": "closing",
  "closingAt": "2000-01-01T02:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-1/2000-01-01T02:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T02:00:00.000Z",
  "volumeId": "my-volume-1",
  "sliceOpenedAt": "2000-01-01T02:00:00.000Z",
  "type": "csv",
  "state": "closing",
  "closingAt": "2000-01-01T03:00:00.000Z",
  %A
}
>>>>>
`
	etcdhelper.AssertKVsString(t, client, expectedEtcdState, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/all/|storage/slice/all/|storage/secret/token/|storage/volume/"))

	// Call CloseAllIn again - no change
	clk.Add(time.Hour)
	require.NoError(t, fileRepo.CloseAllIn(clk.Now(), sinkKey).Do(ctx).Err())
	etcdhelper.AssertKVsString(t, client, expectedEtcdState, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/all/|storage/slice/all/|storage/secret/token/|storage/volume/"))
}

func TestFileRepository_RotateAllIn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T19:00:00.000Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey1 := key.SourceKey{BranchKey: branchKey, SourceID: "my-source-1"}
	sourceKey2 := key.SourceKey{BranchKey: branchKey, SourceID: "my-source-2"}
	sinkKey1 := key.SinkKey{SourceKey: sourceKey1, SinkID: "my-sink-1"}
	sinkKey2 := key.SinkKey{SourceKey: sourceKey1, SinkID: "my-sink-2"}
	sinkKey3 := key.SinkKey{SourceKey: sourceKey1, SinkID: "my-sink-3"}
	sinkKey4 := key.SinkKey{SourceKey: sourceKey2, SinkID: "my-sink-4"}
	sinkKey5 := key.SinkKey{SourceKey: sourceKey2, SinkID: "my-sink-5"}

	// Get services
	d, mocked := dependencies.NewMockedTableSinkScope(t, config.New(), commonDeps.WithClock(clk))
	client := mocked.TestEtcdClient()
	rb := rollback.New(d.Logger())
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	fileFacade := storageRepo.File()
	tokenRepo := storageRepo.Token()

	// Mock file API calls
	transport := mocked.MockedHTTPTransport()
	mockStorageAPICalls(t, clk, branchKey, transport)

	// Create parent branch, source, sinks and tokens
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch).Do(ctx).Err())
		source1 := test.NewSource(sourceKey1)
		require.NoError(t, defRepo.Source().Create("Create source", &source1).Do(ctx).Err())
		source2 := test.NewSource(sourceKey2)
		require.NoError(t, defRepo.Source().Create("Create source", &source2).Do(ctx).Err())
		sink1 := test.NewSink(sinkKey1)
		require.NoError(t, defRepo.Sink().Create("Create sink", &sink1).Do(ctx).Err())
		sink2 := test.NewSink(sinkKey2)
		require.NoError(t, defRepo.Sink().Create("Create sink", &sink2).Do(ctx).Err())
		sink3 := test.NewSink(sinkKey3)
		require.NoError(t, defRepo.Sink().Create("Create sink", &sink3).Do(ctx).Err())
		sink4 := test.NewSink(sinkKey4)
		require.NoError(t, defRepo.Sink().Create("Create sink", &sink4).Do(ctx).Err())
		sink5 := test.NewSink(sinkKey5)
		require.NoError(t, defRepo.Sink().Create("Create sink", &sink5).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink1.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink2.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink3.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink4.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink5.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
	}

	// Create (the first file Rotate)
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, fileFacade.RotateAllIn(rb, clk.Now(), branchKey).Do(ctx).Err())
	}

	// Rotate file (2)
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, fileFacade.RotateAllIn(rb, clk.Now(), branchKey).Do(ctx).Err())
	}

	// Rotate file (3)
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, fileFacade.RotateAllIn(rb, clk.Now(), branchKey).Do(ctx).Err())
	}

	// Check Storage API calls
	// -----------------------------------------------------------------------------------------------------------------
	// File prepare endpoint should be called N times
	assert.Equal(t, 15, transport.GetCallCountInfo()["POST /v2/storage/branch/456/files/prepare"])
	assert.Equal(t, 0, transport.GetCallCountInfo()[`DELETE =~/v2/storage/branch/456/files/\d+$`])

	// Test rollback, delete file endpoint should be called N times
	rb.Invoke(ctx)
	assert.Equal(t, 15, transport.GetCallCountInfo()[`DELETE =~/v2/storage/branch/456/files/\d+$`])

	// Check etcd state
	//   - Only the last file per the Sink is in the storage.FileWriting state.
	//   - Other files per the Sink are in the storage.FileClosing state.
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/file/level/local/123/456/my-source-1/my-sink-1/2000-01-01T20:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-1/my-sink-1/2000-01-01T21:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-1/my-sink-1/2000-01-01T22:00:00.000Z
-----
{
%A
  "state": "writing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-1/my-sink-2/2000-01-01T20:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-1/my-sink-2/2000-01-01T21:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-1/my-sink-2/2000-01-01T22:00:00.000Z
-----
{
%A
  "state": "writing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-1/my-sink-3/2000-01-01T20:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-1/my-sink-3/2000-01-01T21:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-1/my-sink-3/2000-01-01T22:00:00.000Z
-----
{
%A
  "state": "writing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-2/my-sink-4/2000-01-01T20:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-2/my-sink-4/2000-01-01T21:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-2/my-sink-4/2000-01-01T22:00:00.000Z
-----
{
%A
  "state": "writing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-2/my-sink-5/2000-01-01T20:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-2/my-sink-5/2000-01-01T21:00:00.000Z
-----
{
%A
  "state": "closing",
%A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source-2/my-sink-5/2000-01-01T22:00:00.000Z
-----
{
%A
  "state": "writing",
%A
}
>>>>>
`, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/all/|storage/secret/token/"))
}

func TestFileRepository_StateTransition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T00:00:00.000Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}
	volumeID1 := volume.ID("my-volume-1")
	volumeID2 := volume.ID("my-volume-2")

	// Get services
	d, mocked := dependencies.NewMockedTableSinkScope(t, config.New(), commonDeps.WithClock(clk))
	client := mocked.TestEtcdClient()
	rb := rollback.New(d.Logger())
	defRepo := d.DefinitionRepository()
	statsRepo := d.StatisticsRepository()
	storageRepo := d.StorageRepository()
	fileFacade := storageRepo.File()
	sliceFacade := storageRepo.Slice()
	tokenRepo := storageRepo.Token()

	// Mock file API calls
	transport := mocked.MockedHTTPTransport()
	mockStorageAPICalls(t, clk, branchKey, transport)

	// Create parent branch, source, sink and token
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create("Create source", &source).Do(ctx).Err())
		sink := test.NewSink(sinkKey)
		require.NoError(t, defRepo.Sink().Create("Create sink", &sink).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
	}

	// Create (the first file Rotate)
	// -----------------------------------------------------------------------------------------------------------------
	var file storage.File
	{
		clk.Add(time.Hour)
		var err error
		file, err = fileFacade.Rotate(rb, clk.Now(), sinkKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), file.OpenedAt().Time())
	}

	// Create slice1 in the file, in the volume1
	// -----------------------------------------------------------------------------------------------------------------
	var slice1 storage.Slice
	{
		var err error
		clk.Add(time.Hour)
		file1Volume1Key := storage.FileVolumeKey{FileKey: file.FileKey, VolumeID: volumeID1}
		slice1, err = sliceFacade.Rotate(clk.Now(), file1Volume1Key).Do(ctx).ResultOrErr()
		require.NoError(t, err)
	}

	// Create slice2 in the file, in the volume2
	// -----------------------------------------------------------------------------------------------------------------
	var slice2 storage.Slice
	{
		var err error
		clk.Add(time.Hour)
		file1Volume2Key := storage.FileVolumeKey{FileKey: file.FileKey, VolumeID: volumeID2}
		slice2, err = sliceFacade.Rotate(clk.Now(), file1Volume2Key).Do(ctx).ResultOrErr()
		require.NoError(t, err)
	}

	// Put slice statistics values - they should be moved with the file state transitions
	// -----------------------------------------------------------------------------------------------------------------
	require.NoError(t, statsRepo.Put(ctx, []statistics.PerSlice{
		{
			SliceKey: slice1.SliceKey,
			Value: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    slice1.OpenedAt(),
				LastRecordAt:     slice1.OpenedAt().Add(time.Minute),
				RecordsCount:     12,
				UncompressedSize: 10 * datasize.MB,
				CompressedSize:   10 * datasize.MB,
			},
		},
		{
			SliceKey: slice2.SliceKey,
			Value: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    slice2.OpenedAt(),
				LastRecordAt:     slice2.OpenedAt().Add(time.Minute),
				RecordsCount:     34,
				UncompressedSize: 20 * datasize.MB,
				CompressedSize:   20 * datasize.MB,
			},
		},
	}))

	// Switch file to the storage.FileClosing state by StateTransition, it is not possible
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := fileFacade.StateTransition(clk.Now(), file.FileKey, storage.FileWriting, storage.FileClosing).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `unexpected file transition to the state "closing", use Rotate* or Close* methods`, err.Error())
		}
	}

	// Switch file to the storage.FileClosing state by CloseAllIn
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, fileFacade.CloseAllIn(clk.Now(), sinkKey).Do(ctx).Err())
	}

	// Both slices are uploaded
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, sliceFacade.StateTransition(clk.Now(), slice1.SliceKey, storage.SliceClosing, storage.SliceUploading).Do(ctx).Err())
		require.NoError(t, sliceFacade.StateTransition(clk.Now(), slice1.SliceKey, storage.SliceUploading, storage.SliceUploaded).Do(ctx).Err())

		clk.Add(time.Hour)
		require.NoError(t, sliceFacade.StateTransition(clk.Now(), slice2.SliceKey, storage.SliceClosing, storage.SliceUploading).Do(ctx).Err())
		require.NoError(t, sliceFacade.StateTransition(clk.Now(), slice2.SliceKey, storage.SliceUploading, storage.SliceUploaded).Do(ctx).Err())
	}

	// Switch file to the storage.FileImporting state
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, fileFacade.StateTransition(clk.Now(), file.FileKey, storage.FileClosing, storage.FileImporting).Do(ctx).Err())
	}

	// Switch file to the storage.FileImported state
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, fileFacade.StateTransition(clk.Now(), file.FileKey, storage.FileImporting, storage.FileImported).Do(ctx).Err())
	}

	// Check final etcd state
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/file/level/target/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T01:00:00.000Z",
  "type": "csv",
  "state": "imported",
  "closingAt": "2000-01-01T04:00:00.000Z",
  "importingAt": "2000-01-01T07:00:00.000Z",
  "importedAt": "2000-01-01T08:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/slice/level/target/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T02:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T01:00:00.000Z",
  "volumeId": "my-volume-1",
  "sliceOpenedAt": "2000-01-01T02:00:00.000Z",
  "type": "csv",
  "state": "imported",
  "closingAt": "2000-01-01T04:00:00.000Z",
  "uploadingAt": "2000-01-01T05:00:00.000Z",
  "uploadedAt": "2000-01-01T05:00:00.000Z",
  "importedAt": "2000-01-01T08:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/slice/level/target/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T01:00:00.000Z",
  "volumeId": "my-volume-2",
  "sliceOpenedAt": "2000-01-01T03:00:00.000Z",
  "type": "csv",
  "state": "imported",
  "closingAt": "2000-01-01T04:00:00.000Z",
  "uploadingAt": "2000-01-01T06:00:00.000Z",
  "uploadedAt": "2000-01-01T06:00:00.000Z",
  "importedAt": "2000-01-01T08:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/stats/target/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T02:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T02:00:00.000Z",
  "lastRecordAt": "2000-01-01T02:01:00.000Z",
  "recordsCount": 12,
  "uncompressedSize": "10MB",
  "compressedSize": "10MB"
}
>>>>>

<<<<<
storage/stats/target/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-2/2000-01-01T03:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T03:00:00.000Z",
  "lastRecordAt": "2000-01-01T03:01:00.000Z",
  "recordsCount": 34,
  "uncompressedSize": "20MB",
  "compressedSize": "20MB"
}
>>>>>
`, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/all/|storage/slice/all|storage/secret/token/"))
}

func registerWriterVolumes(t *testing.T, ctx context.Context, volumeRepo *repository.VolumeRepository, session *concurrency.Session, count int) {
	t.Helper()
	volumes := []volume.Metadata{
		{
			VolumeID: "my-volume-1",
			Spec:     volume.Spec{NodeID: "node-a", Type: "hdd", Label: "1", Path: "hdd/1"},
		},
		{
			VolumeID: "my-volume-2",
			Spec:     volume.Spec{NodeID: "node-b", Type: "ssd", Label: "2", Path: "ssd/2"},
		},
		{
			VolumeID: "my-volume-3",
			Spec:     volume.Spec{NodeID: "node-b", Type: "hdd", Label: "3", Path: "hdd/3"},
		},
		{
			VolumeID: "my-volume-4",
			Spec:     volume.Spec{NodeID: "node-b", Type: "ssd", Label: "4", Path: "ssd/4"},
		},
		{
			VolumeID: "my-volume-5",
			Spec:     volume.Spec{NodeID: "node-c", Type: "hdd", Label: "5", Path: "hdd/5"},
		},
	}

	if count < 1 || count > 5 {
		panic(errors.New("count must be 1-5"))
	}

	txn := op.Txn(session.Client())
	for _, vol := range volumes[:count] {
		txn.And(volumeRepo.RegisterWriterVolume(vol, session.Lease()))
	}
	require.NoError(t, txn.Do(ctx).Err())
}

func sinkStorageConfig(count int, preferred []string) *storage.ConfigPatch {
	return &storage.ConfigPatch{
		VolumeAssignment: &assignment.Config{
			Count:          count,
			PreferredTypes: preferred,
		},
	}
}

func switchFileStates(t *testing.T, ctx context.Context, clk *clock.Mock, fileRepo *repository.FileRepository, fileKey storage.FileKey, states []storage.FileState) {
	t.Helper()
	from := states[0]
	for _, to := range states[1:] {
		clk.Add(time.Hour)

		// File must be closed by the CloseAllIn method
		var file storage.File
		var err error
		if to == storage.FileClosing {
			require.Equal(t, storage.FileWriting, from)
			require.NoError(t, fileRepo.CloseAllIn(clk.Now(), fileKey.SinkKey).Do(ctx).Err())
			file, err = fileRepo.Get(fileKey).Do(ctx).ResultOrErr()
			require.NoError(t, err)
		} else {
			file, err = fileRepo.StateTransition(clk.Now(), fileKey, from, to).Do(ctx).ResultOrErr()
			require.NoError(t, err)
		}

		// File state has been switched
		assert.Equal(t, to, file.State)

		// Retry should be reset
		assert.Equal(t, 0, file.RetryAttempt)
		assert.Nil(t, file.LastFailedAt)

		// Check timestamp
		switch to {
		case storage.FileClosing:
			assert.Equal(t, utctime.From(clk.Now()).String(), file.ClosingAt.String())
		case storage.FileImporting:
			assert.Equal(t, utctime.From(clk.Now()).String(), file.ImportingAt.String())
		case storage.FileImported:
			assert.Equal(t, utctime.From(clk.Now()).String(), file.ImportedAt.String())
		default:
			panic(errors.Errorf(`unexpected file state "%s"`, to))
		}

		from = to
	}
}
