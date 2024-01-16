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
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/volume"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestSliceRepository_Operations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-03T01:00:00.000Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey1 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}
	sinkKey2 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-2"}
	sinkKey3 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-3"}
	volumeID := volume.ID("my-volume")
	clk.Add(time.Hour)
	fileKey1 := storage.FileKey{SinkKey: sinkKey1, FileID: storage.FileID{OpenedAt: utctime.From(clk.Now())}}
	clk.Add(time.Hour)
	fileKey2 := storage.FileKey{SinkKey: sinkKey2, FileID: storage.FileID{OpenedAt: utctime.From(clk.Now())}}
	clk.Add(time.Hour)
	fileKey3 := storage.FileKey{SinkKey: sinkKey3, FileID: storage.FileID{OpenedAt: utctime.From(clk.Now())}}
	nonExistentSliceKey := storage.SliceKey{
		FileVolumeKey: storage.FileVolumeKey{
			FileKey:  storage.FileKey{SinkKey: sinkKey1, FileID: storage.FileID{OpenedAt: utctime.MustParse("2000-01-02T01:00:00.000Z")}},
			VolumeID: volumeID,
		},
		SliceID: storage.SliceID{OpenedAt: utctime.MustParse("2000-01-02T02:00:00.000Z")},
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

	// Mock file API calls
	transport := mocked.MockedHTTPTransport()
	mockStorageAPICalls(t, clk, branchKey, transport)

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
			assert.Equal(t, `slice "123/456/my-source/my-sink-1/2000-01-02T01:00:00.000Z/my-volume/2000-01-02T02:00:00.000Z" not found in the file`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - parent sink doesn't exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		fileKey := test.NewFileKey()
		fileVolumeKey := storage.FileVolumeKey{FileKey: fileKey, VolumeID: volumeID}
		if err := sliceRepo.Rotate(clk.Now(), fileVolumeKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, strings.TrimSpace(`
- sink "123/456/my-source/my-sink" not found in the source
- file "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z" not found in the sink
`), err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create parent branch, source, sink, tokens and files
	// -----------------------------------------------------------------------------------------------------------------
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

		file1, err := fileRepo.Rotate(rb, fileKey1.OpenedAt().Time(), sink1.SinkKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		fileKey1 = file1.FileKey

		file2, err := fileRepo.Rotate(rb, fileKey2.OpenedAt().Time(), sink2.SinkKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		fileKey2 = file2.FileKey

		file3, err := fileRepo.Rotate(rb, fileKey3.OpenedAt().Time(), sink3.SinkKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		fileKey3 = file3.FileKey
	}

	// Create - parent file doesn't exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		fileKey := test.NewFileKey()
		fileKey.SinkKey = sinkKey1
		fileVolumeKey := storage.FileVolumeKey{FileKey: fileKey, VolumeID: volumeID}
		if err := sliceRepo.Rotate(clk.Now(), fileVolumeKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `file "123/456/my-source/my-sink-1/2000-01-01T19:00:00.000Z" not found in the sink`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - parent file is not in storage.FileWriting state
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, fileRepo.CloseAllIn(clk.Now(), sinkKey3).Do(ctx).Err())

		fileVolumeKey := storage.FileVolumeKey{FileKey: fileKey3, VolumeID: volumeID}
		if err := sliceRepo.Rotate(clk.Now(), fileVolumeKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `slice cannot be created: unexpected file "123/456/my-source/my-sink-3/2000-01-03T04:00:00.000Z" state "closing", expected "writing"`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusBadRequest, err)
		}
	}

	// Create (the first Rotate)
	// See TestRepository_Slice_Rotate for more rotation tests.
	// -----------------------------------------------------------------------------------------------------------------
	var sliceKey1, sliceKey2 storage.SliceKey
	{
		// Create 2 slices in different files
		fileVolumeKey1 := storage.FileVolumeKey{FileKey: fileKey1, VolumeID: volumeID}
		slice1, err := sliceRepo.Rotate(clk.Now(), fileVolumeKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		sliceKey1 = slice1.SliceKey

		fileVolumeKey2 := storage.FileVolumeKey{FileKey: fileKey2, VolumeID: volumeID}
		slice2, err := sliceRepo.Rotate(clk.Now(), fileVolumeKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		sliceKey2 = slice2.SliceKey
	}
	{
		// List
		slices, err := sliceRepo.List(projectID).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, slices, 2)
		slices, err = sliceRepo.List(branchKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, slices, 2)
		slices, err = sliceRepo.List(sourceKey).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, slices, 2)
		slices, err = sliceRepo.List(sinkKey1).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, slices, 1)
		slices, err = sliceRepo.List(fileKey1).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, slices, 1)
		slices, err = sliceRepo.List(fileKey2).Do(ctx).AllKVs()
		assert.NoError(t, err)
		assert.Len(t, slices, 1)
	}
	{
		// Get
		result1, err := sliceRepo.Get(sliceKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), result1.OpenedAt().Time())
		result2, err := sliceRepo.Get(sliceKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), result2.OpenedAt().Time())
	}

	// Create - already exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		fileVolumeKey := storage.FileVolumeKey{FileKey: fileKey2, VolumeID: volumeID}
		if err := sliceRepo.Rotate(clk.Now(), fileVolumeKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `slice "123/456/my-source/my-sink-2/2000-01-03T03:00:00.000Z/my-volume/2000-01-03T04:00:00.000Z" already exists in the file`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusConflict, err)
		}
	}

	// Increment retry attempt - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		err := sliceRepo.IncrementRetry(clk.Now(), nonExistentSliceKey, "some error").Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `slice "123/456/my-source/my-sink-1/2000-01-02T01:00:00.000Z/my-volume/2000-01-02T02:00:00.000Z" not found in the file`, err.Error())
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
		assert.Equal(t, "2000-01-03T05:00:00.000Z", result.LastFailedAt.String())
		assert.Equal(t, "2000-01-03T05:02:00.000Z", result.RetryAfter.String())

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
		assert.Equal(t, `unexpected slice "123/456/my-source/my-sink-1/2000-01-03T02:00:00.000Z/my-volume/2000-01-03T04:00:00.000Z" state transition from "uploaded" to "uploaded"`, err.Error())
		serviceError.AssertErrorStatusCode(t, http.StatusBadRequest, err)
	}

	// Switch slice state - unexpected transition (1)
	// -----------------------------------------------------------------------------------------------------------------
	if err := sliceRepo.StateTransition(clk.Now(), sliceKey1, storage.SliceUploaded, storage.SliceUploading).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `unexpected slice "123/456/my-source/my-sink-1/2000-01-03T02:00:00.000Z/my-volume/2000-01-03T04:00:00.000Z" state transition from "uploaded" to "uploading"`, err.Error())
		serviceError.AssertErrorStatusCode(t, http.StatusBadRequest, err)
	}

	// Switch slice state - unexpected transition (2)
	// -----------------------------------------------------------------------------------------------------------------
	if err := sliceRepo.StateTransition(clk.Now(), sliceKey1, storage.SliceUploaded, storage.SliceImported).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`
unexpected slice "123/456/my-source/my-sink-1/2000-01-03T02:00:00.000Z/my-volume/2000-01-03T04:00:00.000Z" state:
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
			assert.Equal(t, `slice "123/456/my-source/my-sink-2/2000-01-03T03:00:00.000Z/my-volume/2000-01-03T04:00:00.000Z" not found in the file`, err.Error())
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
		assert.Equal(t, `slice "123/456/my-source/my-sink-1/2000-01-02T01:00:00.000Z/my-volume/2000-01-02T02:00:00.000Z" not found in the file`, err.Error())
		serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
	}

	// Check etcd state - slice2 has been deleted, but slice 1 exists
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/slice/all/123/456/my-source/my-sink-1/2000-01-03T02:00:00.000Z/my-volume/2000-01-03T04:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-03T02:00:00.000Z",
  "volumeId": "my-volume",
  "sliceOpenedAt": "2000-01-03T04:00:00.000Z",
  "type": "csv",
  "state": "imported",
  "closingAt": "2000-01-03T06:00:00.000Z",
  "uploadingAt": "2000-01-03T07:00:00.000Z",
  "uploadedAt": "2000-01-03T08:00:00.000Z",
  "importedAt": "2000-01-03T11:00:00.000Z",
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
    "dir": "123/456/my-source/my-sink-1/2000-01-03T02:00:00.000Z/2000-01-03T04:00:00.000Z",
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
    "path": "2000-01-03T04:00:00.000Z_my-volume.gz",
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
storage/slice/level/target/123/456/my-source/my-sink-1/2000-01-03T02:00:00.000Z/my-volume/2000-01-03T04:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-03T02:00:00.000Z",
  "volumeId": "my-volume",
  "sliceOpenedAt": "2000-01-03T04:00:00.000Z",
  "type": "csv",
  "state": "imported",
%A
}
>>>>>
`, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/|storage/secret/token/"))
}

func TestSliceRepository_Rotate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T19:00:00.000Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}
	volumeID := volume.ID("my-volume")

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

	// Create parent branch, source, sink, token and file
	// -----------------------------------------------------------------------------------------------------------------
	var file storage.File
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
	}

	// Create (the first Rotate)
	// -----------------------------------------------------------------------------------------------------------------
	fileVolumeKey := storage.FileVolumeKey{FileKey: file.FileKey, VolumeID: volumeID}
	{
		clk.Add(time.Hour)
		slice1, err := sliceFacade.Rotate(clk.Now(), fileVolumeKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), slice1.OpenedAt().Time())
		assert.Equal(t, 100*datasize.MB, slice1.LocalStorage.AllocatedDiskSpace)
	}

	// Rotate (2)
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		slice2, err := sliceFacade.Rotate(clk.Now(), fileVolumeKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), slice2.OpenedAt().Time())
		assert.Equal(t, 100*datasize.MB, slice2.LocalStorage.AllocatedDiskSpace)
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

	// Check etcd state
	//   - Only the last slice per file and volume is in the storage.SliceWriting state.
	//   - Other slices per file and volume are in the storage.SlicesClosing state.
	//   - AllocatedDiskSpace of the slice5 is 330MB it is 110% of the slice3.
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T20:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T19:00:00.000Z",
  "volumeId": "my-volume",
  "sliceOpenedAt": "2000-01-01T20:00:00.000Z",
  "type": "csv",
  "state": "closing",
  %A
  "local": {
    %A
    "allocatedDiskSpace": "100MB"
  },
  %A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T21:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T19:00:00.000Z",
  "volumeId": "my-volume",
  "sliceOpenedAt": "2000-01-01T21:00:00.000Z",
  "type": "csv",
  "state": "closing",
  %A
  "local": {
    %A
    "allocatedDiskSpace": "100MB"
  },
  %A
}
>>>>>

<<<<<
storage/slice/level/staging/123/456/my-source/my-sink-1/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T22:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T19:00:00.000Z",
  "volumeId": "my-volume",
  "sliceOpenedAt": "2000-01-01T22:00:00.000Z",
  "type": "csv",
  "state": "uploaded",
  "closingAt": "2000-01-01T23:00:00.000Z",
  "uploadingAt": "2000-01-02T00:00:00.000Z",
  "uploadedAt": "2000-01-02T01:00:00.000Z",
  %A
  "local": {
    %A
    "allocatedDiskSpace": "100MB"
  },
  %A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T23:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T19:00:00.000Z",
  "volumeId": "my-volume",
  "sliceOpenedAt": "2000-01-01T23:00:00.000Z",
  "type": "csv",
  "state": "closing",
  "closingAt": "2000-01-02T02:00:00.000Z",
  %A
  "local": {
    %A
    "allocatedDiskSpace": "100MB"
  },
  %A
}
>>>>>

<<<<<
storage/slice/level/local/123/456/my-source/my-sink-1/2000-01-01T19:00:00.000Z/my-volume/2000-01-02T02:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T19:00:00.000Z",
  "volumeId": "my-volume",
  "sliceOpenedAt": "2000-01-02T02:00:00.000Z",
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
`, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/|storage/slice/all/|storage/stats/|storage/secret/token/"))
}

func TestSliceRepository_StateTransition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T19:00:00.000Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}
	volumeID := volume.ID("my-volume")

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

	// Mock file API calls
	transport := mocked.MockedHTTPTransport()
	mockStorageAPICalls(t, clk, branchKey, transport)

	// Create parent branch, source, sink and file
	// -----------------------------------------------------------------------------------------------------------------
	var file storage.File
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
	}

	// Create (the first Rotate)
	// -----------------------------------------------------------------------------------------------------------------
	var slice storage.Slice
	fileVolumeKey := storage.FileVolumeKey{FileKey: file.FileKey, VolumeID: volumeID}
	{
		var err error
		clk.Add(time.Hour)
		slice, err = sliceRepo.Rotate(clk.Now(), fileVolumeKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), slice.OpenedAt().Time())
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
		require.NoError(t, sliceRepo.Close(clk.Now(), fileVolumeKey).Do(ctx).Err())
	}

	// Switch slice to the storage.SliceUploading state
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, sliceRepo.StateTransition(clk.Now(), slice.SliceKey, storage.SliceClosing, storage.SliceUploading).Do(ctx).Err())
	}

	// Switch slice to the storage.SliceUploaded state
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, sliceRepo.StateTransition(clk.Now(), slice.SliceKey, storage.SliceUploading, storage.SliceUploaded).Do(ctx).Err())
	}

	// Try switch slice to the storage.SliceImported state - it is not possible, file is in the storage.FileWriting
	// -----------------------------------------------------------------------------------------------------------------
	{
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

	// Check final etcd state
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/file/level/target/123/456/my-source/my-sink-1/2000-01-01T19:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T19:00:00.000Z",
  "type": "csv",
  "state": "imported",
  "closingAt": "2000-01-02T01:00:00.000Z",
  "importingAt": "2000-01-02T02:00:00.000Z",
  "importedAt": "2000-01-02T03:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/slice/level/target/123/456/my-source/my-sink-1/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T20:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T19:00:00.000Z",
  "volumeId": "my-volume",
  "sliceOpenedAt": "2000-01-01T20:00:00.000Z",
  "type": "csv",
  "state": "imported",
  "closingAt": "2000-01-01T21:00:00.000Z",
  "uploadingAt": "2000-01-01T22:00:00.000Z",
  "uploadedAt": "2000-01-01T23:00:00.000Z",
  "importedAt": "2000-01-02T03:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/stats/target/123/456/my-source/my-sink-1/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T20:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T20:00:00.000Z",
  "lastRecordAt": "2000-01-01T20:01:00.000Z",
  "recordsCount": 123,
  "uncompressedSize": "100MB",
  "compressedSize": "100MB"
}
>>>>>
`, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/all/|storage/slice/all/|storage/secret/token/"))
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
