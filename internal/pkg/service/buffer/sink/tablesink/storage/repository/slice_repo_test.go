package repository

import (
	"context"
	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/s3"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	defRepository "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test"
	deps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/relvacode/iso8601"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestRepository_Slice(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	now := utctime.MustParse("2000-01-03T01:00:00.000Z").Time()
	clk := clock.NewMock()
	clk.Set(now)

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}
	volumeID := storage.VolumeID("my-volume")
	cfg := storage.NewConfig()
	credentials := &keboola.FileUploadCredentials{
		S3UploadParams: &s3.UploadParams{
			Credentials: s3.Credentials{
				Expiration: iso8601.Time{Time: now.Add(time.Hour)},
			},
		},
	}
	nonExistentSliceKey := storage.SliceKey{
		FileKey: storage.FileKey{SinkKey: sinkKey, FileID: storage.FileID{OpenedAt: utctime.MustParse("2000-01-02T01:00:00.000Z")}},
		SliceID: storage.SliceID{VolumeID: volumeID, OpenedAt: utctime.MustParse("2000-01-02T02:00:00.000Z")},
	}

	d := deps.NewMocked(t, deps.WithEnabledEtcdClient(), deps.WithClock(clk))
	client := d.TestEtcdClient()
	defRepo := defRepository.New(d)
	backoff := storage.NoRandomizationBackoff()
	r := newWithBackoff(d, defRepo, cfg, backoff).Slice()

	// Empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		// List - empty
		slices, err := r.List(projectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, slices)
		slices, err = r.List(sinkKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, slices)
	}
	{
		// Get - not found
		if err := r.Get(nonExistentSliceKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `slice "123/456/my-source/my-sink/2000-01-02T01:00:00.000Z/my-volume/2000-01-02T02:00:00.000Z" not found in the file`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - parent sink doesn't exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		fileKey := test.NewFileKey()
		if err := r.Create(fileKey, volumeID).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "123/456/my-source/my-sink" not found in the source`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create parent branch, source, sink and files
	// -----------------------------------------------------------------------------------------------------------------
	var fileKey1, fileKey2, fileKey3 storage.FileKey
	{
		branch := branchTemplate(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch).Do(ctx).Err())
		source := sourceTemplate(sourceKey)
		require.NoError(t, defRepo.Source().Create("Create source", &source).Do(ctx).Err())
		sink := sinkTemplate(sinkKey)
		require.NoError(t, defRepo.Sink().Create("Create sink", &sink).Do(ctx).Err())

		clk.Add(time.Hour)
		file1, err := r.all.file.Create(sink.SinkKey, credentials).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		fileKey1 = file1.FileKey

		clk.Add(time.Hour)
		file2, err := r.all.file.Create(sink.SinkKey, credentials).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		fileKey2 = file2.FileKey

		clk.Add(time.Hour)
		file3, err := r.all.file.Create(sink.SinkKey, credentials).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		fileKey3 = file3.FileKey
	}

	// Create - parent file doesn't exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		fileKey := test.NewFileKey()
		fileKey.SinkKey = sinkKey
		if err := r.Create(fileKey, volumeID).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `file "123/456/my-source/my-sink/2000-01-01T19:00:00.000Z" not found in the sink`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - parent file is not in storage.FileWriting state
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, r.all.File().StateTransition(fileKey3, storage.FileClosing).Do(ctx).Err())

		if err := r.Create(fileKey3, volumeID).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `slice cannot be created: unexpected file "123/456/my-source/my-sink/2000-01-03T04:00:00.000Z" state "closing", expected "writing"`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusBadRequest, err)
		}
	}

	// Create
	// -----------------------------------------------------------------------------------------------------------------
	var sliceKey1, sliceKey2 storage.SliceKey
	{
		// Create 2 slices in different files
		slice1, err := r.Create(fileKey1, volumeID).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		sliceKey1 = slice1.SliceKey

		slice2, err := r.Create(fileKey2, volumeID).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		sliceKey2 = slice2.SliceKey
	}
	{
		// List
		slices, err := r.List(projectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, slices, 2)
		slices, err = r.List(branchKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, slices, 2)
		slices, err = r.List(sourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, slices, 2)
		slices, err = r.List(sinkKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, slices, 2)
		slices, err = r.List(fileKey1).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, slices, 1)
		slices, err = r.List(fileKey2).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, slices, 1)
	}
	{
		// Get
		result1, err := r.Get(sliceKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), result1.Value.OpenedAt().Time())
		result2, err := r.Get(sliceKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), result2.Value.OpenedAt().Time())
	}

	// Create - already exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := r.Create(fileKey2, volumeID).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `slice "123/456/my-source/my-sink/2000-01-03T03:00:00.000Z/my-volume/2000-01-03T04:00:00.000Z" already exists in the file`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusConflict, err)
		}
	}

	// Update (update is private method, public methods IncrementRetry and StateTransition are tested bellow)
	// -----------------------------------------------------------------------------------------------------------------
	clk.Add(time.Hour)
	{
		// Increment retry
		result, err := r.update(sliceKey1, func(v storage.Slice) (storage.Slice, error) {
			v.LocalStorage.DiskSync.Wait = false
			return v, nil
		}).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.False(t, result.LocalStorage.DiskSync.Wait)
		kv, err := r.Get(sliceKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, result, kv.Value)
	}

	// Update - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := r.update(nonExistentSliceKey, func(v storage.Slice) (storage.Slice, error) {
			return v, nil
		}).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `slice "123/456/my-source/my-sink/2000-01-02T01:00:00.000Z/my-volume/2000-01-02T02:00:00.000Z" not found in the file`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Increment retry attempt
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := r.IncrementRetry(sliceKey1, "some error").Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, 1, result.RetryAttempt)
		assert.Equal(t, "some error", result.RetryReason)
		assert.Equal(t, "2000-01-03T05:00:00.000Z", result.LastFailedAt.String())
		assert.Equal(t, "2000-01-03T05:02:00.000Z", result.RetryAfter.String())
		kv, err := r.Get(sliceKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, result, kv.Value)
	}

	// Switch slice state
	// -----------------------------------------------------------------------------------------------------------------
	for _, to := range []storage.SliceState{storage.SliceClosing, storage.SliceUploading, storage.SliceUploaded} {
		clk.Add(time.Hour)

		result, err := r.StateTransition(sliceKey1, to).Do(ctx).ResultOrErr()
		require.NoError(t, err)

		// Slice state has been switched
		assert.Equal(t, to, result.State)

		// Retry should be reset
		assert.Equal(t, 0, result.RetryAttempt)
		assert.Nil(t, result.LastFailedAt)

		// Check timestamp
		switch to {
		case storage.SliceClosing:
			assert.Equal(t, utctime.From(clk.Now()).String(), result.ClosingAt.String())
		case storage.SliceUploading:
			assert.Equal(t, utctime.From(clk.Now()).String(), result.UploadingAt.String())
		case storage.SliceUploaded:
			assert.Equal(t, utctime.From(clk.Now()).String(), result.UploadedAt.String())
		case storage.SliceImported:
			assert.Equal(t, utctime.From(clk.Now()).String(), result.ImportedAt.String())
		}
	}

	// Switch slice state - already in the state
	// -----------------------------------------------------------------------------------------------------------------
	if err := r.StateTransition(sliceKey1, storage.SliceUploaded).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `unexpected slice "123/456/my-source/my-sink/2000-01-03T02:00:00.000Z/my-volume/2000-01-03T04:00:00.000Z" state transition from "uploaded" to "uploaded"`, err.Error())
		serviceError.AssertErrorStatusCode(t, http.StatusBadRequest, err)
	}

	// Switch slice state - unexpected transition (1)
	// -----------------------------------------------------------------------------------------------------------------
	if err := r.StateTransition(sliceKey1, storage.SliceUploading).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `unexpected slice "123/456/my-source/my-sink/2000-01-03T02:00:00.000Z/my-volume/2000-01-03T04:00:00.000Z" state transition from "uploaded" to "uploading"`, err.Error())
		serviceError.AssertErrorStatusCode(t, http.StatusBadRequest, err)
	}

	// Switch slice state - unexpected transition (2)
	// -----------------------------------------------------------------------------------------------------------------
	if err := r.StateTransition(sliceKey1, storage.SliceImported).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`
unexpected slice "123/456/my-source/my-sink/2000-01-03T02:00:00.000Z/my-volume/2000-01-03T04:00:00.000Z" state:
- unexpected combination: file state "writing" and slice state "imported"
`), err.Error())
		serviceError.AssertErrorStatusCode(t, http.StatusBadRequest, err)
	}

	// Switch file state
	// -----------------------------------------------------------------------------------------------------------------
	{
		for _, to := range []storage.FileState{storage.FileClosing, storage.FileImporting, storage.FileImported} {
			clk.Add(time.Hour)

			result, err := r.all.File().StateTransition(fileKey1, to).Do(ctx).ResultOrErr()
			require.NoError(t, err)

			// File state has been switched
			assert.Equal(t, to, result.State)

			// Retry fields should be reset
			assert.Equal(t, 0, result.RetryAttempt)
			assert.Nil(t, result.LastFailedAt)

			// Check timestamp
			switch to {
			case storage.FileClosing:
				assert.Equal(t, utctime.From(clk.Now()).String(), result.ClosingAt.String())
			case storage.FileImporting:
				assert.Equal(t, utctime.From(clk.Now()).String(), result.ImportingAt.String())
			case storage.FileImported:
				assert.Equal(t, utctime.From(clk.Now()).String(), result.ImportedAt.String())
			}
		}
	}

	// Delete
	// -----------------------------------------------------------------------------------------------------------------
	{
		assert.NoError(t, r.Delete(sliceKey2).Do(ctx).Err())
	}
	{
		// Get - not found
		if err := r.Get(sliceKey2).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `slice "123/456/my-source/my-sink/2000-01-03T03:00:00.000Z/my-volume/2000-01-03T04:00:00.000Z" not found in the file`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// List - empty
		slices, err := r.List(fileKey2).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, slices)
	}

	// Delete - not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := r.Delete(nonExistentSliceKey).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `slice "123/456/my-source/my-sink/2000-01-02T01:00:00.000Z/my-volume/2000-01-02T02:00:00.000Z" not found in the file`, err.Error())
		serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
	}

	// Check etcd state - slice2 has been deleted, but slice 1 exists
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsString(t, client, expectedSliceEtcdState(), etcdhelper.WithIgnoredKeyPattern("^(definition|storage/file)/"))
}

func expectedSliceEtcdState() string {
	return `
<<<<<
storage/slice/all/123/456/my-source/my-sink/2000-01-03T02:00:00.000Z/my-volume/2000-01-03T04:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink",
  "fileOpenedAt": "2000-01-03T02:00:00.000Z",
  "sliceOpenedAt": "2000-01-03T04:00:00.000Z",
  "volumeId": "my-volume",
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
    "dir": "123/456/my-source/my-sink/2000-01-03T02:00:00.000Z/2000-01-03T04:00:00.000Z",
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
    "allocatedDiskSpace": "110MB"
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
storage/slice/level/target/123/456/my-source/my-sink/2000-01-03T02:00:00.000Z/my-volume/2000-01-03T04:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink",
  "fileOpenedAt": "2000-01-03T02:00:00.000Z",
  "sliceOpenedAt": "2000-01-03T04:00:00.000Z",
  "volumeId": "my-volume",
  "type": "csv",
  "state": "imported",
%A
}
>>>>>
`
}

func TestNewSlice_InvalidCompressionType(t *testing.T) {
	t.Parallel()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}
	now := utctime.MustParse("2000-01-01T19:00:00.000Z").Time()
	cfg := storage.NewConfig()
	mapping := definition.TableMapping{
		TableID: keboola.MustParseTableID("in.bucket.table"),
		Columns: column.Columns{
			column.Datetime{Name: "datetime"},
			column.Body{Name: "body"},
		},
	}
	credentials := &keboola.FileUploadCredentials{
		S3UploadParams: &s3.UploadParams{
			Credentials: s3.Credentials{
				Expiration: iso8601.Time{Time: now.Add(time.Hour)},
			},
		},
	}

	// Create file
	file, err := newFile(now, cfg, sinkKey, mapping, credentials)
	require.NoError(t, err)

	// Set unsupported compression type
	file.LocalStorage.Compression.Type = compression.TypeZSTD

	// Assert
	_, err = newSlice(now, file, "my-volume", 0)
	require.Error(t, err)
	assert.Equal(t, `file compression type "zstd" is not supported`, err.Error())
}
