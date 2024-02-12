package repository_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

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
	test.MockCreateFilesStorageAPICalls(t, clk, branchKey, transport)
	test.MockDeleteFilesStorageAPICalls(t, branchKey, transport)

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		test.RegisterWriterVolumes(t, ctx, volumeRepo, session, 1)
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
  "compressedSize": "100MB",
  "stagingSize": "100MB"
}
>>>>>
`, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/all/|storage/slice/all/|storage/secret/token/|storage/volume/"))
}
