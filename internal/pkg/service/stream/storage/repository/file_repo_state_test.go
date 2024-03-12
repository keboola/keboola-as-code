package repository_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

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

	// Get services
	d, mocked := dependencies.NewMockedLocalStorageScope(t, commonDeps.WithClock(clk))
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
	test.MockCreateFilesStorageAPICalls(t, clk, branchKey, transport)
	test.MockDeleteFilesStorageAPICalls(t, branchKey, transport)

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		test.RegisterWriterVolumes(t, ctx, volumeRepo, session, 2)
	}

	// Create parent branch, source, sink and token
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(clk.Now(), &branch).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(clk.Now(), "Create source", &source).Do(ctx).Err())
		sink := test.NewSink(sinkKey)
		sink.Config = sink.Config.With(testconfig.LocalVolumeConfig(2, []string{"ssd"}))
		require.NoError(t, defRepo.Sink().Create(clk.Now(), "Create sink", &sink).Do(ctx).Err())
		require.NoError(t, tokenRepo.Put(sink.SinkKey, keboola.Token{Token: "my-token"}).Do(ctx).Err())
	}

	// Create (the first file Rotate)
	// -----------------------------------------------------------------------------------------------------------------
	var file model.File
	{
		clk.Add(time.Hour)
		var err error
		file, err = fileRepo.Rotate(rb, clk.Now(), sinkKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, clk.Now(), file.OpenedAt().Time())
	}

	// Get file slices
	// -----------------------------------------------------------------------------------------------------------------
	var sliceKey1, sliceKey2 model.SliceKey
	{
		slices, err := sliceRepo.ListIn(file.FileKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, slices, 2)
		sliceKey1 = slices[0].SliceKey
		sliceKey2 = slices[1].SliceKey
	}

	// Put slice statistics values - they should be moved with the file state transitions
	// -----------------------------------------------------------------------------------------------------------------
	require.NoError(t, statsRepo.Put(ctx, []statistics.PerSlice{
		{
			SliceKey: sliceKey1,
			Value: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    sliceKey1.OpenedAt(),
				LastRecordAt:     sliceKey1.OpenedAt().Add(time.Minute),
				RecordsCount:     12,
				UncompressedSize: 10 * datasize.MB,
				CompressedSize:   10 * datasize.MB,
			},
		},
		{
			SliceKey: sliceKey2,
			Value: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    sliceKey2.OpenedAt(),
				LastRecordAt:     sliceKey2.OpenedAt().Add(time.Minute),
				RecordsCount:     34,
				UncompressedSize: 20 * datasize.MB,
				CompressedSize:   20 * datasize.MB,
			},
		},
	}))

	// Switch file to the storage.FileClosing state by StateTransition, it is not possible
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := fileRepo.StateTransition(clk.Now(), file.FileKey, model.FileWriting, model.FileClosing).Do(ctx).Err()
		if assert.Error(t, err) {
			assert.Equal(t, `unexpected file transition to the state "closing", use Rotate* or Close* methods`, err.Error())
		}
	}

	// Switch file to the storage.FileClosing state by CloseAllIn
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, fileRepo.CloseAllIn(clk.Now(), sinkKey).Do(ctx).Err())
	}

	// Both slices are uploaded
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, sliceRepo.StateTransition(clk.Now(), sliceKey1, model.SliceClosing, model.SliceUploading).Do(ctx).Err())
		require.NoError(t, sliceRepo.StateTransition(clk.Now(), sliceKey1, model.SliceUploading, model.SliceUploaded).Do(ctx).Err())

		clk.Add(time.Hour)
		require.NoError(t, sliceRepo.StateTransition(clk.Now(), sliceKey2, model.SliceClosing, model.SliceUploading).Do(ctx).Err())
		require.NoError(t, sliceRepo.StateTransition(clk.Now(), sliceKey2, model.SliceUploading, model.SliceUploaded).Do(ctx).Err())
	}

	// Switch file to the storage.FileImporting state
	// -----------------------------------------------------------------------------------------------------------------
	var toImportingEtcdLogs string
	{
		etcdLogs.Reset()
		clk.Add(time.Hour)
		require.NoError(t, fileRepo.StateTransition(clk.Now(), file.FileKey, model.FileClosing, model.FileImporting).Do(ctx).Err())
		toImportingEtcdLogs = etcdLogs.String()
	}

	// Switch file to the storage.FileImported state
	// -----------------------------------------------------------------------------------------------------------------
	var toImportedEtcdLogs string
	{
		etcdLogs.Reset()
		clk.Add(time.Hour)
		require.NoError(t, fileRepo.StateTransition(clk.Now(), file.FileKey, model.FileImporting, model.FileImported).Do(ctx).Err())
		toImportedEtcdLogs = etcdLogs.String()
	}

	// Check etcd state
	// -----------------------------------------------------------------------------------------------------------------
	etcdlogger.Assert(t, `
// File StateTransition - AtomicOp - Read Phase
➡️  TXN
  ➡️  THEN:
  // File
  001 ➡️  GET "storage/file/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z"
  // All slices in file
  002 ➡️  GET ["storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/", "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z0")
  // Local statistics
  003 ➡️  GET ["storage/stats/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/", "storage/stats/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z0")
✔️  TXN | succeeded: true

// File StateTransition - AtomicOp - Write Phase
➡️  TXN
  ➡️  IF:
  // File
  001 "storage/file/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z" MOD GREATER 0
  002 "storage/file/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z" MOD LESS %d
  // All slices in file
  003 ["storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/", "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z0") MOD GREATER 0
  004 ["storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/", "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z0") MOD LESS %d
  005 "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z" MOD GREATER 0
  006 "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-2/2000-01-01T01:00:00.000Z" MOD GREATER 0
  // Local statistics
  007 ["storage/stats/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/", "storage/stats/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z0") MOD EQUAL 0
  ➡️  THEN:
  // Save modified file
  001 ➡️  PUT "storage/file/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z"
  002 ➡️  PUT "storage/file/level/staging/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z"
  003 ➡️  DEL "storage/file/level/local/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z"
✔️  TXN | succeeded: true
`, toImportingEtcdLogs)
	etcdlogger.Assert(t, `
// File StateTransition - AtomicOp - Read Phase
➡️  TXN
  ➡️  THEN:
  // File
  001 ➡️  GET "storage/file/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z"
  // All slices in file
  002 ➡️  GET ["storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/", "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z0")
  // Staging statistics
  003 ➡️  GET ["storage/stats/staging/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/", "storage/stats/staging/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z0")
✔️  TXN | succeeded: true

// File StateTransition - AtomicOp - Write Phase
➡️  TXN
  ➡️  IF:
  // File
  001 "storage/file/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z" MOD GREATER 0
  002 "storage/file/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z" MOD LESS %d
  // All slices in file
  003 ["storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/", "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z0") MOD GREATER 0
  004 ["storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/", "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z0") MOD LESS %d
  005 "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z" MOD GREATER 0
  006 "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-2/2000-01-01T01:00:00.000Z" MOD GREATER 0
  // Staging statistics
  007 ["storage/stats/staging/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/", "storage/stats/staging/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z0") MOD GREATER 0
  008 ["storage/stats/staging/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/", "storage/stats/staging/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z0") MOD LESS %d
  009 "storage/stats/staging/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z/value" MOD GREATER 0
  010 "storage/stats/staging/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-2/2000-01-01T01:00:00.000Z/value" MOD GREATER 0
  ➡️  THEN:
  // Save modified file and slices - in two copies, in "all" and <level> prefix
  001 ➡️  PUT "storage/file/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z"
  002 ➡️  PUT "storage/file/level/target/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z"
  003 ➡️  DEL "storage/file/level/staging/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z"
  004 ➡️  PUT "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z"
  005 ➡️  PUT "storage/slice/level/target/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z"
  006 ➡️  DEL "storage/slice/level/staging/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z"
  007 ➡️  PUT "storage/slice/all/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-2/2000-01-01T01:00:00.000Z"
  008 ➡️  PUT "storage/slice/level/target/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-2/2000-01-01T01:00:00.000Z"
  009 ➡️  DEL "storage/slice/level/staging/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-2/2000-01-01T01:00:00.000Z"
  // Move file statistics
  010 ➡️  PUT "storage/stats/target/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z/value"
  011 ➡️  DEL "storage/stats/staging/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-1/2000-01-01T01:00:00.000Z/value"
  012 ➡️  PUT "storage/stats/target/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-2/2000-01-01T01:00:00.000Z/value"
  013 ➡️  DEL "storage/stats/staging/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-2/2000-01-01T01:00:00.000Z/value"
✔️  TXN | succeeded: true
`, toImportedEtcdLogs)

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
  "closingAt": "2000-01-01T02:00:00.000Z",
  "importingAt": "2000-01-01T05:00:00.000Z",
  "importedAt": "2000-01-01T06:00:00.000Z",
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
  "uploadedAt": "2000-01-01T03:00:00.000Z",
  "importedAt": "2000-01-01T06:00:00.000Z",
  %A
}
>>>>>

<<<<<
storage/slice/level/target/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-2/2000-01-01T01:00:00.000Z
-----
{
  "projectId": 123,
  "branchId": 456,
  "sourceId": "my-source",
  "sinkId": "my-sink-1",
  "fileOpenedAt": "2000-01-01T01:00:00.000Z",
  "volumeId": "my-volume-2",
  "sliceOpenedAt": "2000-01-01T01:00:00.000Z",
  "type": "csv",
  "state": "imported",
  "closingAt": "2000-01-01T02:00:00.000Z",
  "uploadingAt": "2000-01-01T04:00:00.000Z",
  "uploadedAt": "2000-01-01T04:00:00.000Z",
  "importedAt": "2000-01-01T06:00:00.000Z",
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
  "recordsCount": 12,
  "uncompressedSize": "10MB",
  "compressedSize": "10MB",
  "stagingSize": "10MB"
}
>>>>>

<<<<<
storage/stats/target/123/456/my-source/my-sink-1/2000-01-01T01:00:00.000Z/my-volume-2/2000-01-01T01:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T01:00:00.000Z",
  "lastRecordAt": "2000-01-01T01:01:00.000Z",
  "recordsCount": 34,
  "uncompressedSize": "20MB",
  "compressedSize": "20MB",
  "stagingSize": "20MB"
}
>>>>>
`, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/all/|storage/slice/all|storage/secret/token/|storage/volume/"))
}
