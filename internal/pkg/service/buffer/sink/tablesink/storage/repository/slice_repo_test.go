package repository_test

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

func TestSliceRepository_Operations(t *testing.T) {
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
	sinkKey3 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-3"}
	nonExistentSliceKey := storage.SliceKey{
		FileVolumeKey: storage.FileVolumeKey{
			FileKey:  storage.FileKey{SinkKey: sinkKey1, FileID: storage.FileID{OpenedAt: utctime.MustParse("2000-01-01T01:02:03.000Z")}},
			VolumeID: "my-volume",
		},
		SliceID: storage.SliceID{OpenedAt: utctime.MustParse("2000-01-01T01:02:03.000Z")},
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
		registerWriterVolumes(t, ctx, volumeRepo, session, 1)
	}

	// Empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		// List - empty
		slices, err := sliceRepo.List(projectID).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, slices)
		slices, err = sliceRepo.List(sinkKey1).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, slices)
	}
	{
		// Get - not found
		if err := sliceRepo.Get(nonExistentSliceKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `slice "123/456/my-source/my-sink-1/2000-01-01T01:02:03.000Z/my-volume/2000-01-01T01:02:03.000Z" not found in the file`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - parent sink doesn't exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		fileKey := test.NewFileKey()
		fileVolumeKey := storage.FileVolumeKey{FileKey: fileKey, VolumeID: "my-volume"}
		if err := sliceRepo.Rotate(clk.Now(), fileVolumeKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, strings.TrimSpace(`
- sink "123/456/my-source/my-sink" not found in the source
- file "123/456/my-source/my-sink/2000-01-01T01:00:00.000Z" not found in the sink
`), err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create parent branch, source, sink, tokens and files
	// -----------------------------------------------------------------------------------------------------------------
	var fileKey1, fileKey2, fileKey3 storage.FileKey
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create("Create source", &source).Do(ctx).Err())
		sink1 := test.NewSink(sinkKey1)
		require.NoError(t, defRepo.Sink().Create("Create sink", &sink1).Do(ctx).Err())
		sink2 := test.NewSink(sinkKey2)
		require.NoError(t, defRepo.Sink().Create("Create sink", &sink2).Do(ctx).Err())
		sink3 := test.NewSink(sinkKey3)
		require.NoError(t, defRepo.Sink().Create("Create sink", &sink3).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink1.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink2.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink3.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())

		file1, err := fileRepo.Rotate(rb, clk.Now(), sink1.SinkKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		fileKey1 = file1.FileKey

		file2, err := fileRepo.Rotate(rb, clk.Now(), sink2.SinkKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		fileKey2 = file2.FileKey

		file3, err := fileRepo.Rotate(rb, clk.Now(), sink3.SinkKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		fileKey3 = file3.FileKey
	}

	// Check slices created by the FileRepository.Rotate
	// -----------------------------------------------------------------------------------------------------------------
	var sliceKey1, sliceKey2, sliceKey3 storage.SliceKey
	{
		slices, err := sliceRepo.List(sourceKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, slices, 3)
		sliceKey1 = slices[0].SliceKey
		sliceKey2 = slices[1].SliceKey
		sliceKey3 = slices[2].SliceKey
	}
	{
		// List
		slices, err := sliceRepo.List(projectID).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, slices, 3)
		slices, err = sliceRepo.List(branchKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, slices, 3)
		slices, err = sliceRepo.List(sourceKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, slices, 3)
		slices, err = sliceRepo.List(sinkKey1).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, slices, 1)
		slices, err = sliceRepo.List(sinkKey2).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, slices, 1)
		slices, err = sliceRepo.List(sinkKey3).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, slices, 1)
		slices, err = sliceRepo.List(fileKey1).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, slices, 1)
		slices, err = sliceRepo.List(fileKey2).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, slices, 1)
		slices, err = sliceRepo.List(fileKey3).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, slices, 1)
	}
	{
		// Get
		result1, err := sliceRepo.Get(sliceKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, "2000-01-01T01:00:00.000Z", result1.OpenedAt().String())
		result2, err := sliceRepo.Get(sliceKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, "2000-01-01T01:00:00.000Z", result2.OpenedAt().String())
		result3, err := sliceRepo.Get(sliceKey3).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, "2000-01-01T01:00:00.000Z", result3.OpenedAt().String())
	}

	// Rotate - parent file doesn't exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		fileKey := storage.FileKey{SinkKey: sinkKey1, FileID: storage.FileID{OpenedAt: utctime.MustParse("2000-01-01T04:05:06.000Z")}}
		fileVolumeKey := storage.FileVolumeKey{FileKey: fileKey, VolumeID: "my-volume"}
		if err := sliceRepo.Rotate(clk.Now(), fileVolumeKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `file "123/456/my-source/my-sink-1/2000-01-01T04:05:06.000Z" not found in the sink`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Rotate - parent file is not in storage.FileWriting state
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, fileRepo.CloseAllIn(clk.Now(), sinkKey3).Do(ctx).Err())

		fileVolumeKey := storage.FileVolumeKey{FileKey: fileKey3, VolumeID: sliceKey3.VolumeID}
		if err := sliceRepo.Rotate(clk.Now(), fileVolumeKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `slice cannot be created: unexpected file "123/456/my-source/my-sink-3/2000-01-01T01:00:00.000Z" state "closing", expected "writing"`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusBadRequest, err)
		}
	}

	// Rotate - already exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		fileVolumeKey := storage.FileVolumeKey{FileKey: fileKey2, VolumeID: sliceKey2.VolumeID}
		if err := sliceRepo.Rotate(clk.Now(), fileVolumeKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `slice "123/456/my-source/my-sink-2/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z" already exists in the file`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusConflict, err)
		}
	}

	// Increment retry attempt - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		err := sliceRepo.IncrementRetry(clk.Now(), nonExistentSliceKey, "some error").Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `slice "123/456/my-source/my-sink-1/2000-01-01T01:02:03.000Z/my-volume/2000-01-01T01:02:03.000Z" not found in the file`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Increment retry attempt
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := sliceRepo.IncrementRetry(clk.Now(), sliceKey1, "some error").Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, 1, result.RetryAttempt)
		assert.Equal(t, "some error", result.RetryReason)
		assert.Equal(t, "2000-01-01T02:00:00.000Z", result.LastFailedAt.String())
		assert.Equal(t, "2000-01-01T02:02:00.000Z", result.RetryAfter.String())

		slice1, err := sliceRepo.Get(sliceKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, result, slice1)
	}

	// Switch slice state
	// -----------------------------------------------------------------------------------------------------------------
	switchSliceStates(t, ctx, clk, sliceRepo, sliceKey1, []storage.SliceState{
		storage.SliceWriting, storage.SliceClosing, storage.SliceUploading, storage.SliceUploaded,
	})

	// Switch slice state - already in the state
	// -----------------------------------------------------------------------------------------------------------------
	if err := sliceRepo.StateTransition(clk.Now(), sliceKey1, storage.SliceUploaded, storage.SliceUploaded).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `unexpected slice "123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z" state transition from "uploaded" to "uploaded"`, err.Error())
		serviceError.AssertErrorStatusCode(t, http.StatusBadRequest, err)
	}

	// Switch slice state - unexpected transition (1)
	// -----------------------------------------------------------------------------------------------------------------
	if err := sliceRepo.StateTransition(clk.Now(), sliceKey1, storage.SliceUploaded, storage.SliceUploading).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `unexpected slice "123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z" state transition from "uploaded" to "uploading"`, err.Error())
		serviceError.AssertErrorStatusCode(t, http.StatusBadRequest, err)
	}

	// Switch slice state - unexpected transition (2)
	// -----------------------------------------------------------------------------------------------------------------
	if err := sliceRepo.StateTransition(clk.Now(), sliceKey1, storage.SliceUploaded, storage.SliceImported).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`
unexpected slice "123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z" state:
- unexpected combination: file state "writing" and slice state "imported"
`), err.Error())
		serviceError.AssertErrorStatusCode(t, http.StatusBadRequest, err)
	}

	// Switch file state
	// -----------------------------------------------------------------------------------------------------------------
	switchFileStates(t, ctx, clk, fileRepo, fileKey1, []storage.FileState{
		storage.FileWriting, storage.FileClosing, storage.FileImporting, storage.FileImported,
	})

	// Delete
	// -----------------------------------------------------------------------------------------------------------------
	{
		assert.NoError(t, sliceRepo.Delete(sliceKey2).Do(ctx).Err())
	}
	{
		// Get - not found
		if err := sliceRepo.Get(sliceKey2).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `slice "123/456/my-source/my-sink-2/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z" not found in the file`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// List - empty
		slices, err := sliceRepo.List(fileKey2).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Empty(t, slices)
	}

	// Delete - not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := sliceRepo.Delete(nonExistentSliceKey).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `slice "123/456/my-source/my-sink-1/2000-01-01T01:02:03.000Z/my-volume/2000-01-01T01:02:03.000Z" not found in the file`, err.Error())
		serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
	}

	// Check etcd state - slice2 has been deleted, but slice 1 exists
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z
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
  "state": "imported",
  "closingAt": "2000-01-01T03:00:00.000Z",
  "uploadingAt": "2000-01-01T04:00:00.000Z",
  "uploadedAt": "2000-01-01T05:00:00.000Z",
  "importedAt": "2000-01-01T08:00:00.000Z",
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
  "local": {
    "dir": "123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/2000-01-01T01:00:00.000Z",
    "filename": "slice.csv.gz",
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
      "wait": false,
      "checkInterval": 1000000,
      "countTrigger": 100,
      "bytesTrigger": "100KB",
      "intervalTrigger": 100000000
    },
    "allocatedDiskSpace": "100MB"
  },
  "staging": {
    "path": "2000-01-01T01:00:00.000Z_my-volume-1.gz",
    "compression": {
      "type": "gzip",
      "gzip": {
        "level": 1,
        "implementation": "parallel",
        "blockSize": "256KB",
        "concurrency": 0
      }
    }
  }
}
>>>>>

<<<<<
storage/slice/all/123/456/my-source/my-sink-3/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-3",
  "fileOpenedAt": "2000-01-01T01:00:00.000Z",
  "volumeId": "my-volume-1",
  "sliceOpenedAt": "2000-01-01T01:00:00.000Z",
  "type": "csv",
  "state": "closing",
  "closingAt": "2000-01-01T01:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/slice/level/target/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z
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
  "state": "imported",
  "closingAt": "2000-01-01T03:00:00.000Z",
  "uploadingAt": "2000-01-01T04:00:00.000Z",
  "uploadedAt": "2000-01-01T05:00:00.000Z",
  "importedAt": "2000-01-01T08:00:00.000Z",
%A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-3/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-3",
  "fileOpenedAt": "2000-01-01T01:00:00.000Z",
  "volumeId": "my-volume-1",
  "sliceOpenedAt": "2000-01-01T01:00:00.000Z",
  "type": "csv",
  "state": "closing",
  "closingAt": "2000-01-01T01:00:00.000Z",
%A
}
>>>>>
`, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/|storage/secret/token/|storage/volume/"))
}

func TestSliceRepository_Rotate(t *testing.T) {
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
	statsRepo := d.StatisticsRepository()
	storageRepo := d.StorageRepository()
	fileFacade := storageRepo.File()
	sliceFacade := storageRepo.Slice()
	tokenRepo := storageRepo.Token()
	volumeRepo := storageRepo.Volume()

	// Log etcd operations
	var etcdLogs bytes.Buffer
	rawClient := d.EtcdClient()
	rawClient.KV = etcdlogger.KVLogWrapper(rawClient.KV, &etcdLogs, etcdlogger.WithMinimal())

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

	// Create parent branch, source, sink, token, file and slice1
	// -----------------------------------------------------------------------------------------------------------------
	var file storage.File
	var fileVolumeKey storage.FileVolumeKey
	{
		var err error
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create("Create source", &source).Do(ctx).Err())
		sink := test.NewSink(sinkKey)
		require.NoError(t, defRepo.Sink().Create("Create sink", &sink).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
		file, err = fileFacade.Rotate(rb, clk.Now(), sinkKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		slices, err := sliceFacade.List(file.FileKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, slices, 1)
		slice := slices[0]
		assert.Equal(t, clk.Now(), slice.OpenedAt().Time())
		assert.Equal(t, 100*datasize.MB, slice.LocalStorage.AllocatedDiskSpace)
		fileVolumeKey = slice.FileVolumeKey
	}

	// Rotate (2)
	// -----------------------------------------------------------------------------------------------------------------
	var rotateEtcdLogs string
	{
		etcdLogs.Reset()
		clk.Add(time.Hour)
		slice2, err := sliceFacade.Rotate(clk.Now(), fileVolumeKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), slice2.OpenedAt().Time())
		assert.Equal(t, 100*datasize.MB, slice2.LocalStorage.AllocatedDiskSpace)
		rotateEtcdLogs = etcdLogs.String()
	}

	// Rotate (3)
	// -----------------------------------------------------------------------------------------------------------------
	var slice3 storage.Slice
	{
		var err error
		clk.Add(time.Hour)
		slice3, err = sliceFacade.Rotate(clk.Now(), fileVolumeKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), slice3.OpenedAt().Time())
		assert.Equal(t, 100*datasize.MB, slice3.LocalStorage.AllocatedDiskSpace)
	}

	// Rotate (4)
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		slice4, err := sliceFacade.Rotate(clk.Now(), fileVolumeKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), slice4.OpenedAt().Time())
		assert.Equal(t, 100*datasize.MB, slice4.LocalStorage.AllocatedDiskSpace)
	}

	// Move slice3 to the storage.SliceUploaded state
	// -----------------------------------------------------------------------------------------------------------------
	slice3DiskSize := 300 * datasize.MB
	{
		// Save slice statistics
		require.NoError(t, statsRepo.Put(ctx, []statistics.PerSlice{
			{
				SliceKey: slice3.SliceKey,
				Value: statistics.Value{
					SlicesCount:      1,
					FirstRecordAt:    slice3.OpenedAt(),
					LastRecordAt:     slice3.OpenedAt().Add(time.Minute),
					RecordsCount:     123,
					UncompressedSize: 500 * datasize.MB,
					CompressedSize:   slice3DiskSize,
					StagingSize:      200 * datasize.MB,
				},
			},
		}))

		// Closing -> Uploading -> Uploaded
		clk.Add(time.Hour)
		require.NoError(t, sliceFacade.StateTransition(clk.Now(), slice3.SliceKey, storage.SliceClosing, storage.SliceUploading).Do(ctx).Err())
		clk.Add(time.Hour)
		require.NoError(t, sliceFacade.StateTransition(clk.Now(), slice3.SliceKey, storage.SliceUploading, storage.SliceUploaded).Do(ctx).Err())
	}

	// Rotate (5) - AllocatedDiskSpace is calculated from the uploaded slice3 statistics
	// -----------------------------------------------------------------------------------------------------------------
	{
		expectedSlice5Size := int(slice3DiskSize) * file.LocalStorage.DiskAllocation.SizePercent / 100
		clk.Add(time.Hour)
		slice5, err := sliceFacade.Rotate(clk.Now(), fileVolumeKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), slice5.OpenedAt().Time())
		assert.Equal(t, datasize.ByteSize(expectedSlice5Size), slice5.LocalStorage.AllocatedDiskSpace)
	}

	// Check etcd logs
	// -----------------------------------------------------------------------------------------------------------------
	etcdlogger.Assert(t, `
// Slice Rotate - AtomicOp - Read Phase
➡️  TXN
  ➡️  THEN:
  // Sink
  001 ➡️  GET "definition/sink/active/123/456/my-source/my-sink-1"
  // File
  002 ➡️  GET "storage/file/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z"
  // All local slices on the volume
  003 ➡️  GET ["storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/", "storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-10")
✔️  TXN | succeeded: true

// Slice Rotate - MaxUsedDiskSizeBySliceIn
➡️  TXN
  ➡️  THEN:
  001 ➡️  GET ["storage/stats/staging/123/456/my-source/my-sink-1/", "storage/stats/staging/123/456/my-source/my-sink-10")
  002 ➡️  GET ["storage/stats/target/123/456/my-source/my-sink-1/", "storage/stats/target/123/456/my-source/my-sink-10")
✔️  TXN | succeeded: true

// Slice Rotate - AtomicOp - Write Phase
➡️  TXN
  ➡️  IF:
  // Sink
  001 "definition/sink/active/123/456/my-source/my-sink-1" MOD GREATER 0
  002 "definition/sink/active/123/456/my-source/my-sink-1" MOD LESS %d
  // File
  003 "storage/file/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z" MOD GREATER 0
  004 "storage/file/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z" MOD LESS %d
  // All local slices on the volume
  005 ["storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/", "storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-10") MOD GREATER 0
  006 ["storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/", "storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-10") MOD LESS %d
  007 "storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z" MOD GREATER 0
  // New slice must not exist
  008 "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T02:00:00.000Z" MOD EQUAL 0
  ➡️  THEN:
  // Save opened and closed slice - in two copies, in "all" and <level> prefix
  001 ➡️  PUT "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T02:00:00.000Z"
  002 ➡️  PUT "storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T02:00:00.000Z"
  003 ➡️  PUT "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z"
  004 ➡️  PUT "storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z"
  ➡️  ELSE:
  001 ➡️  TXN
  001   ➡️  IF:
  001   001 "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T02:00:00.000Z" MOD EQUAL 0
  001   ➡️  ELSE:
  001   001 ➡️  TXN
  001   001   ➡️  IF:
  001   001   001 "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T02:00:00.000Z" MOD EQUAL 0
✔️  TXN | succeeded: true
`, rotateEtcdLogs)

	// Check etcd state
	//   - Only the last slice per file and volume is in the storage.SliceWriting state.
	//   - Other slices per file and volume are in the storage.SlicesClosing state.
	//   - AllocatedDiskSpace of the slice5 is 330MB it is 110% of the slice3.
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsString(t, client, `
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
  "local": {
    %A
    "allocatedDiskSpace": "100MB"
  },
  %A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T02:00:00.000Z
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
  "state": "closing",
  "closingAt": "2000-01-01T03:00:00.000Z",
  %A
  "local": {
    %A
    "allocatedDiskSpace": "100MB"
  },
  %A
}
>>>>>

<<<<<
storage/slice/level/staging/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T03:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T01:00:00.000Z",
  "volumeId": "my-volume-1",
  "sliceOpenedAt": "2000-01-01T03:00:00.000Z",
  "type": "csv",
  "state": "uploaded",
  "closingAt": "2000-01-01T04:00:00.000Z",
  "uploadingAt": "2000-01-01T05:00:00.000Z",
  "uploadedAt": "2000-01-01T06:00:00.000Z",
  %A
  "local": {
    %A
    "allocatedDiskSpace": "100MB"
  },
  %A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T04:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T01:00:00.000Z",
  "volumeId": "my-volume-1",
  "sliceOpenedAt": "2000-01-01T04:00:00.000Z",
  "type": "csv",
  "state": "closing",
  "closingAt": "2000-01-01T07:00:00.000Z",
  %A
  "local": {
    %A
    "allocatedDiskSpace": "100MB"
  },
  %A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T07:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T01:00:00.000Z",
  "volumeId": "my-volume-1",
  "sliceOpenedAt": "2000-01-01T07:00:00.000Z",
  "type": "csv",
  "state": "writing",
  %A
  "local": {
    %A
    "allocatedDiskSpace": "330MB"
  },
  %A
}
>>>>>
`, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/|storage/slice/all/|storage/stats/|storage/secret/token/|storage/volume"))
}

func TestSliceRepository_StateTransition(t *testing.T) {
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
	statsRepo := d.StatisticsRepository()
	storageRepo := d.StorageRepository()
	fileRepo := storageRepo.File()
	sliceRepo := storageRepo.Slice()
	tokenRepo := storageRepo.Token()
	volumeRepo := storageRepo.Volume()

	// Log etcd operations
	var etcdLogs bytes.Buffer
	rawClient := d.EtcdClient()
	rawClient.KV = etcdlogger.KVLogWrapper(rawClient.KV, &etcdLogs, etcdlogger.WithMinimal())

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

	// Create parent branch, source, sink, file and slice
	// -----------------------------------------------------------------------------------------------------------------
	var file storage.File
	var slice storage.Slice
	{
		var err error
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create("Create source", &source).Do(ctx).Err())
		sink := test.NewSink(sinkKey)
		require.NoError(t, defRepo.Sink().Create("Create sink", &sink).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
		file, err = fileRepo.Rotate(rb, clk.Now(), sinkKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		slices, err := sliceRepo.List(file.FileKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, slices, 1)
		slice = slices[0]
	}

	// Put slice statistics value - it should be moved with the slice state transitions
	// -----------------------------------------------------------------------------------------------------------------
	require.NoError(t, statsRepo.Put(ctx, []statistics.PerSlice{
		{
			SliceKey: slice.SliceKey,
			Value: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    slice.OpenedAt(),
				LastRecordAt:     slice.OpenedAt().Add(time.Minute),
				RecordsCount:     123,
				UncompressedSize: 100 * datasize.MB,
				CompressedSize:   100 * datasize.MB,
			},
		},
	}))

	// Switch slice to the storage.SliceClosing state by StateTransition, it is not possible
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := sliceRepo.StateTransition(clk.Now(), slice.SliceKey, storage.SliceWriting, storage.SliceClosing).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `unexpected transition to the state "closing", use Rotate or Close method`, err.Error())
		}
	}

	// Switch slice to the storage.SliceClosing state by Close
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, sliceRepo.Close(clk.Now(), slice.FileVolumeKey).Do(ctx).Err())
	}

	// Switch slice to the storage.SliceUploading state
	// -----------------------------------------------------------------------------------------------------------------
	var toUploadingEtcdLogs string
	{
		etcdLogs.Reset()
		clk.Add(time.Hour)
		require.NoError(t, sliceRepo.StateTransition(clk.Now(), slice.SliceKey, storage.SliceClosing, storage.SliceUploading).Do(ctx).Err())
		toUploadingEtcdLogs = etcdLogs.String()
	}

	// Switch slice to the storage.SliceUploaded state
	// -----------------------------------------------------------------------------------------------------------------
	var toUploadedEtcdLogs string
	{
		etcdLogs.Reset()
		clk.Add(time.Hour)
		require.NoError(t, sliceRepo.StateTransition(clk.Now(), slice.SliceKey, storage.SliceUploading, storage.SliceUploaded).Do(ctx).Err())
		toUploadedEtcdLogs = etcdLogs.String()
	}

	// Try switch slice to the storage.SliceImported state - it is not possible, file is in the storage.FileWriting
	// -----------------------------------------------------------------------------------------------------------------
	{
		etcdLogs.Reset()
		clk.Add(time.Hour)
		err := sliceRepo.StateTransition(clk.Now(), slice.SliceKey, storage.SliceUploaded, storage.SliceImported).Do(ctx).Err()
		if assert.Error(t, err) {
			wildcards.Assert(t, "unexpected slice \"%s\" state:\n- unexpected combination: file state \"writing\" and slice state \"imported\"", err.Error())
		}
	}

	// Switch slice to the storage.SliceImported state, together with the file to the storage.FileImported state
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, fileRepo.CloseAllIn(clk.Now(), sinkKey).Do(ctx).Err())
		clk.Add(time.Hour)
		require.NoError(t, fileRepo.StateTransition(clk.Now(), file.FileKey, storage.FileClosing, storage.FileImporting).Do(ctx).Err())
		clk.Add(time.Hour)
		require.NoError(t, fileRepo.StateTransition(clk.Now(), file.FileKey, storage.FileImporting, storage.FileImported).Do(ctx).Err())
	}

	// Check etcd logs
	// -----------------------------------------------------------------------------------------------------------------
	etcdlogger.Assert(t, `
// Slice StateTransition - AtomicOp - Read Phase
➡️  TXN
  ➡️  THEN:
  // Slice
  001 ➡️  GET "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z"
  // File
  002 ➡️  GET "storage/file/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z"
✔️  TXN | succeeded: true

// Slice StateTransition - AtomicOp - Write Phase
➡️  TXN
  ➡️  IF:
  // Slice
  001 "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z" MOD GREATER 0
  002 "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z" MOD LESS %d
  // File
  003 "storage/file/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z" MOD GREATER 0
  004 "storage/file/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z" MOD LESS %d
  ➡️  THEN:
  // Save modified slice - in two copies, in "all" and <level> prefix
  001 ➡️  PUT "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z"
  002 ➡️  PUT "storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z"
✔️  TXN | succeeded: true
`, toUploadingEtcdLogs)
	etcdlogger.Assert(t, `
➡️  TXN
  ➡️  THEN:
  // Slice
  001 ➡️  GET "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z"
  // File
  002 ➡️  GET "storage/file/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z"
  // Local statistics
  003 ➡️  GET "storage/stats/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z/value"
✔️  TXN | succeeded: true

➡️  TXN
  ➡️  IF:
  // Slice
  001 "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z" MOD GREATER 0
  002 "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z" MOD LESS %d
  // File
  003 "storage/file/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z" MOD GREATER 0
  004 "storage/file/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z" MOD LESS %d
  // Local statistics
  005 "storage/stats/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z/value" MOD GREATER 0
  006 "storage/stats/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z/value" MOD LESS %d
  ➡️  THEN:
  // Save modified slice - in two copies, in "all" and <level> prefix
  001 ➡️  PUT "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z"
  002 ➡️  PUT "storage/slice/level/staging/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z"
  003 ➡️  DEL "storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z"
  // Move statistics
  004 ➡️  PUT "storage/stats/staging/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z/value"
  005 ➡️  DEL "storage/stats/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z/value"
✔️  TXN | succeeded: true
`, toUploadedEtcdLogs)

	// Check etcd state
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
  "closingAt": "2000-01-01T06:00:00.000Z",
  "importingAt": "2000-01-01T07:00:00.000Z",
  "importedAt": "2000-01-01T08:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/slice/level/target/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z
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
  "state": "imported",
  "closingAt": "2000-01-01T02:00:00.000Z",
  "uploadingAt": "2000-01-01T03:00:00.000Z",
  "uploadedAt": "2000-01-01T04:00:00.000Z",
  "importedAt": "2000-01-01T08:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/stats/target/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T01:00:00.000Z",
  "lastRecordAt": "2000-01-01T01:01:00.000Z",
  "recordsCount": 123,
  "uncompressedSize": "100MB",
  "compressedSize": "100MB"
}
>>>>>
`, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/all/|storage/slice/all/|storage/secret/token/|storage/volume/"))
}

func switchSliceStates(t *testing.T, ctx context.Context, clk *clock.Mock, sliceRepo *repository.SliceRepository, sliceKey storage.SliceKey, states []storage.SliceState) {
	t.Helper()
	from := states[0]
	for _, to := range states[1:] {
		clk.Add(time.Hour)

		// Slice must be closed by the Close method
		var slice storage.Slice
		var err error
		if to == storage.SliceClosing {
			require.NoError(t, sliceRepo.Close(clk.Now(), sliceKey.FileVolumeKey).Do(ctx).Err())
			slice, err = sliceRepo.Get(sliceKey).Do(ctx).ResultOrErr()
			require.NoError(t, err)
		} else {
			slice, err = sliceRepo.StateTransition(clk.Now(), sliceKey, from, to).Do(ctx).ResultOrErr()
			require.NoError(t, err)
		}

		// Slice state has been switched
		assert.Equal(t, to, slice.State)

		// Retry should be reset
		assert.Equal(t, 0, slice.RetryAttempt)
		assert.Nil(t, slice.LastFailedAt)

		// Check timestamp
		switch to {
		case storage.SliceClosing:
			assert.Equal(t, utctime.From(clk.Now()).String(), slice.ClosingAt.String())
		case storage.SliceUploading:
			assert.Equal(t, utctime.From(clk.Now()).String(), slice.UploadingAt.String())
		case storage.SliceUploaded:
			assert.Equal(t, utctime.From(clk.Now()).String(), slice.UploadedAt.String())
		case storage.SliceImported:
			assert.Equal(t, utctime.From(clk.Now()).String(), slice.ImportedAt.String())
		default:
			panic(errors.Errorf(`unexpected slice state "%s"`, to))
		}

		from = to
	}
}
