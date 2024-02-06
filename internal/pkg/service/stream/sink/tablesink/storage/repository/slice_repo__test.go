package repository_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
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
      "checkInterval": "1ms",
      "countTrigger": 100,
      "bytesTrigger": "100KB",
      "intervalTrigger": "100ms"
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
