package repository

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/s3"
	"github.com/relvacode/iso8601"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	defRepository "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/disksync"
	deps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func branchTemplate(k key.BranchKey) definition.Branch {
	return definition.Branch{BranchKey: k}
}

func sourceTemplate(k key.SourceKey) definition.Source {
	return definition.Source{
		SourceKey:   k,
		Type:        definition.SourceTypeHTTP,
		Name:        "My Source",
		Description: "My Description",
		HTTP:        &definition.HTTPSource{Secret: "012345678901234567890123456789012345678912345678"},
	}
}

func sinkTemplate(k key.SinkKey) definition.Sink {
	return definition.Sink{
		SinkKey:     k,
		Type:        definition.SinkTypeTable,
		Name:        "My Sink",
		Description: "My Description",
		Table: &definition.TableSink{
			Storage: &storage.ConfigPatch{
				Local: &local.ConfigPatch{
					DiskSync: &disksync.Config{
						Mode:            disksync.ModeDisk,
						Wait:            true,
						CheckInterval:   1 * time.Millisecond,
						CountTrigger:    100,
						BytesTrigger:    100 * datasize.KB,
						IntervalTrigger: 100 * time.Millisecond,
					},
				},
			},
			Mapping: definition.TableMapping{
				TableID: keboola.MustParseTableID("in.bucket.table"),
				Columns: column.Columns{
					column.Datetime{Name: "datetime"},
					column.Body{Name: "body"},
				},
			},
		},
	}
}

func TestRepository_File(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	now := utctime.MustParse("2000-01-01T19:00:00.000Z").Time()
	clk := clock.NewMock()
	clk.Set(now)

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey1 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}
	sinkKey2 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-2"}
	fileKey1 := storage.FileKey{SinkKey: sinkKey1, FileID: storage.FileID{OpenedAt: utctime.From(now)}}
	fileKey2 := storage.FileKey{SinkKey: sinkKey2, FileID: storage.FileID{OpenedAt: utctime.From(now)}}
	credentials := &keboola.FileUploadCredentials{
		S3UploadParams: &s3.UploadParams{
			Credentials: s3.Credentials{
				Expiration: iso8601.Time{Time: now.Add(time.Hour)},
			},
		},
	}
	nonExistentFileKey := storage.FileKey{
		SinkKey: sinkKey1,
		FileID:  storage.FileID{OpenedAt: utctime.MustParse("2000-01-01T18:00:00.000Z")},
	}

	d := deps.NewMocked(t, deps.WithEnabledEtcdClient(), deps.WithClock(clk))
	client := d.TestEtcdClient()
	defRepo := defRepository.New(d)
	cfg := storage.NewConfig()
	backoff := storage.NoRandomizationBackoff()
	r := newWithBackoff(d, defRepo, cfg, backoff).File()

	// Empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		// List - empty
		files, err := r.List(projectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, files)
		files, err = r.List(sinkKey1).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, files)
	}
	{
		// Get - not found
		if err := r.Get(nonExistentFileKey).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `file "123/456/my-source/my-sink-1/2000-01-01T18:00:00.000Z" not found in the sink`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create - parent sink doesn't exists
	// -----------------------------------------------------------------------------------------------------------------
	// Entity exists only in memory
	{
		if err := r.Create(fileKey1, credentials).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `sink "123/456/my-source/my-sink-1" not found in the source`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Create parent branch, source and sinks
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := branchTemplate(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch).Do(ctx).Err())
		source := sourceTemplate(sourceKey)
		require.NoError(t, defRepo.Source().Create("Create source", &source).Do(ctx).Err())
		sink1 := sinkTemplate(sinkKey1)
		require.NoError(t, defRepo.Sink().Create("Create sink", &sink1).Do(ctx).Err())
		sink2 := sinkTemplate(sinkKey2)
		require.NoError(t, defRepo.Sink().Create("Create sink", &sink2).Do(ctx).Err())
	}

	// Create
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Create 2 files in different sinks
		file1, err := r.Create(fileKey1, credentials).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, credentials, file1.StagingStorage.UploadCredentials)

		file2, err := r.Create(fileKey2, credentials).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, credentials, file2.StagingStorage.UploadCredentials)
	}
	{
		// List
		files, err := r.List(projectID).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, files, 2)
		files, err = r.List(branchKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, files, 2)
		files, err = r.List(sourceKey).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, files, 2)
		files, err = r.List(sinkKey1).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, files, 1)
		files, err = r.List(sinkKey2).Do(ctx).All()
		assert.NoError(t, err)
		assert.Len(t, files, 1)
	}
	{
		// Get
		result1, err := r.Get(fileKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, now, result1.OpenedAt().Time())
		result2, err := r.Get(fileKey2).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, now, result2.OpenedAt().Time())
	}

	// Create - already exists
	// -----------------------------------------------------------------------------------------------------------------
	{
		if err := r.Create(fileKey1, credentials).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `file "123/456/my-source/my-sink-1/2000-01-01T19:00:00.000Z" already exists in the sink`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusConflict, err)
		}
	}

	// Update (readAndUpdate is private method, public methods IncrementRetry and StateTransition are tested bellow)
	// -----------------------------------------------------------------------------------------------------------------
	clk.Add(time.Hour)
	{
		// Modify configuration
		result, err := r.readAndUpdate(fileKey1, func(v storage.File) (storage.File, error) {
			v.LocalStorage.DiskSync.Wait = false
			return v, nil
		}).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.False(t, result.LocalStorage.DiskSync.Wait)

		file1, err := r.Get(fileKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, result, file1)
	}

	// Update - not found
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := r.readAndUpdate(nonExistentFileKey, func(v storage.File) (storage.File, error) {
			return v, nil
		}).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `file "123/456/my-source/my-sink-1/2000-01-01T18:00:00.000Z" not found in the sink`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}

	// Increment retry attempt
	// -----------------------------------------------------------------------------------------------------------------
	{
		result, err := r.IncrementRetry(fileKey1, "some error").Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, 1, result.RetryAttempt)
		assert.Equal(t, "some error", result.RetryReason)
		assert.Equal(t, "2000-01-01T20:00:00.000Z", result.LastFailedAt.String())
		assert.Equal(t, "2000-01-01T20:02:00.000Z", result.RetryAfter.String())

		file1, err := r.Get(fileKey1).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, result, file1)
	}

	// Switch file state
	// -----------------------------------------------------------------------------------------------------------------
	for _, to := range []storage.FileState{storage.FileClosing, storage.FileImporting, storage.FileImported} {
		clk.Add(time.Hour)

		result, err := r.StateTransition(fileKey1, to).Do(ctx).ResultOrErr()
		require.NoError(t, err)

		// File state has been switched
		assert.Equal(t, to, result.State)

		// Retry should be reset
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

	// Switch file state - already in the state
	// -----------------------------------------------------------------------------------------------------------------
	if err := r.StateTransition(fileKey1, storage.FileImported).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `unexpected file "123/456/my-source/my-sink-1/2000-01-01T19:00:00.000Z" state transition from "imported" to "imported"`, err.Error())
		serviceError.AssertErrorStatusCode(t, http.StatusBadRequest, err)
	}

	// Switch file state - unexpected transition
	// -----------------------------------------------------------------------------------------------------------------
	if err := r.StateTransition(fileKey1, storage.FileImporting).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `unexpected file "123/456/my-source/my-sink-1/2000-01-01T19:00:00.000Z" state transition from "imported" to "importing"`, err.Error())
		serviceError.AssertErrorStatusCode(t, http.StatusBadRequest, err)
	}

	// Delete
	// -----------------------------------------------------------------------------------------------------------------
	{
		assert.NoError(t, r.Delete(fileKey2).Do(ctx).Err())
	}
	{
		// Get - not found
		if err := r.Get(fileKey2).Do(ctx).Err(); assert.Error(t, err) {
			assert.Equal(t, `file "123/456/my-source/my-sink-2/2000-01-01T19:00:00.000Z" not found in the sink`, err.Error())
			serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
		}
	}
	{
		// List - empty
		files, err := r.List(sinkKey2).Do(ctx).All()
		assert.NoError(t, err)
		assert.Empty(t, files)
	}

	// Delete - not found
	// -----------------------------------------------------------------------------------------------------------------
	if err := r.Delete(nonExistentFileKey).Do(ctx).Err(); assert.Error(t, err) {
		assert.Equal(t, `file "123/456/my-source/my-sink-1/2000-01-01T18:00:00.000Z" not found in the sink`, err.Error())
		serviceError.AssertErrorStatusCode(t, http.StatusNotFound, err)
	}

	// Check etcd state - file2 has been deleted, but file 1 exists
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsString(t, client, expectedFileEtcdState(), etcdhelper.WithIgnoredKeyPattern("^definition/"))
}

func expectedFileEtcdState() string {
	return `
<<<<<
storage/file/all/123/456/my-source/my-sink-1/2000-01-01T19:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T19:00:00.000Z",
  "type": "csv",
  "state": "imported",
  "closingAt": "2000-01-01T21:00:00.000Z",
  "importingAt": "2000-01-01T22:00:00.000Z",
  "importedAt": "2000-01-01T23:00:00.000Z",
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
    "dir": "123/456/my-source/my-sink-1/2000-01-01T19:00:00.000Z",
    "compression": {
      "type": "gzip",
      "gzip": {
        "level": 1,
        "implementation": "parallel",
        "blockSize": "256KB",
        "concurrency": 0
      }
    },
    "volumesAssignment": {
      "perPod": 1,
      "preferredTypes": [
        "default"
      ]
    },
    "diskSync": {
      "mode": "disk",
      "wait": false,
      "checkInterval": 1000000,
      "countTrigger": 100,
      "bytesTrigger": "100KB",
      "intervalTrigger": 100000000
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
    "credentialsExpiration": "2000-01-01T20:00:00.000Z"
  },
  "target": {
    "tableId": "in.bucket.table"
  }
}
>>>>>

<<<<<
storage/file/level/target/123/456/my-source/my-sink-1/2000-01-01T19:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T19:00:00.000Z",
%A
}
>>>>>
`
}

func TestRepository_File_Create_InTheSameSink(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	now := utctime.MustParse("2000-01-01T19:00:00.000Z").Time()
	clk := clock.NewMock()
	clk.Set(now)

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}
	credentials := &keboola.FileUploadCredentials{
		S3UploadParams: &s3.UploadParams{
			Credentials: s3.Credentials{
				Expiration: iso8601.Time{Time: now.Add(time.Hour)},
			},
		},
	}

	d := deps.NewMocked(t, deps.WithEnabledEtcdClient(), deps.WithClock(clk))
	client := d.TestEtcdClient()
	defRepo := defRepository.New(d)
	cfg := storage.NewConfig()
	backoff := storage.NoRandomizationBackoff()
	r := newWithBackoff(d, defRepo, cfg, backoff).File()

	// Create sink
	branch := branchTemplate(branchKey)
	require.NoError(t, defRepo.Branch().Create(&branch).Do(ctx).Err())
	source := sourceTemplate(sourceKey)
	require.NoError(t, defRepo.Source().Create("Create source", &source).Do(ctx).Err())
	sink := sinkTemplate(sinkKey)
	require.NoError(t, defRepo.Sink().Create("Create sink", &sink).Do(ctx).Err())

	// Create 3 files
	clk.Add(time.Hour)
	fileKey1 := storage.FileKey{SinkKey: sinkKey, FileID: storage.FileID{OpenedAt: utctime.From(clk.Now())}}
	require.NoError(t, r.Create(fileKey1, credentials).Do(ctx).Err())
	clk.Add(time.Hour)
	fileKey2 := storage.FileKey{SinkKey: sinkKey, FileID: storage.FileID{OpenedAt: utctime.From(clk.Now())}}
	require.NoError(t, r.Create(fileKey2, credentials).Do(ctx).Err())
	clk.Add(time.Hour)
	fileKey3 := storage.FileKey{SinkKey: sinkKey, FileID: storage.FileID{OpenedAt: utctime.From(clk.Now())}}
	require.NoError(t, r.Create(fileKey3, credentials).Do(ctx).Err())

	// Old file is always switched from the FileWriting state, to the FileClosing state
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/file/level/local/123/456/my-source/my-sink-1/2000-01-01T20:00:00.000Z
-----
{
  %A
  "fileOpenedAt": "2000-01-01T20:00:00.000Z",
  %A
  "state": "closing",
  "closingAt": "2000-01-01T21:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source/my-sink-1/2000-01-01T21:00:00.000Z
-----
{
  %A
  "fileOpenedAt": "2000-01-01T21:00:00.000Z",
  %A
  "state": "closing",
  "closingAt": "2000-01-01T22:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/file/level/local/123/456/my-source/my-sink-1/2000-01-01T22:00:00.000Z
-----
{
  %A
  "fileOpenedAt": "2000-01-01T22:00:00.000Z",
  %A
  "state": "writing",
  %A
}
>>>>>
`, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/all"))
}

func TestNewFile_InvalidCompressionType(t *testing.T) {
	t.Parallel()

	// Fixtures
	now := utctime.MustParse("2000-01-01T19:00:00.000Z").Time()
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}
	fileKey := storage.FileKey{SinkKey: sinkKey, FileID: storage.FileID{OpenedAt: utctime.From(now)}}
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

	// Set unsupported compression type
	cfg.Local.Compression.Type = compression.TypeZSTD

	// Assert
	_, err := newFile(fileKey, cfg, mapping, credentials)
	require.Error(t, err)
	assert.Equal(t, `file compression type "zstd" is not supported`, err.Error())
}
