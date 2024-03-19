package slice_test

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
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

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
	d, mocked := dependencies.NewMockedLocalStorageScope(t, commonDeps.WithClock(clk))
	client := mocked.TestEtcdClient()
	rb := rollback.New(d.Logger())
	defRepo := d.DefinitionRepository()
	statsRepo := d.StatisticsRepository()
	storageRepo := d.StorageRepository()
	fileRepo := storageRepo.File()
	sliceFacade := storageRepo.Slice()
	volumeRepo := storageRepo.Volume()

	// Simulate that the operation is running in an API request authorized by a token
	api := d.KeboolaPublicAPI().WithToken(mocked.StorageAPIToken().Token)
	ctx = context.WithValue(ctx, dependencies.KeboolaProjectAPICtxKey, api)

	// Log etcd operations
	var etcdLogs bytes.Buffer
	rawClient := d.EtcdClient()
	rawClient.KV = etcdlogger.KVLogWrapper(rawClient.KV, &etcdLogs, etcdlogger.WithMinimal())

	// Mock file API calls
	transport := mocked.MockedHTTPTransport()
	test.MockBucketStorageAPICalls(t, transport)
	test.MockTableStorageAPICalls(t, transport)
	test.MockTokenStorageAPICalls(t, transport)
	test.MockFileStorageAPICalls(t, clk, transport)

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		test.RegisterWriterVolumes(t, ctx, volumeRepo, session, 1)
	}

	// Create parent branch, source, sink, token, file and slice1
	// -----------------------------------------------------------------------------------------------------------------
	var file model.File
	var fileVolumeKey model.FileVolumeKey
	{
		var err error
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(rb, clk.Now(), &branch).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(rb, clk.Now(), "Create source", &source).Do(ctx).Err())
		sink := test.NewSink(sinkKey)
		require.NoError(t, defRepo.Sink().Create(rb, clk.Now(), "Create sink", &sink).Do(ctx).Err())
		file, err = fileRepo.Rotate(rb, clk.Now(), sinkKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		slices, err := sliceFacade.ListIn(file.FileKey).Do(ctx).All()
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
	var slice3 model.Slice
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
		require.NoError(t, sliceFacade.StateTransition(clk.Now(), slice3.SliceKey, model.SliceClosing, model.SliceUploading).Do(ctx).Err())
		clk.Add(time.Hour)
		require.NoError(t, sliceFacade.StateTransition(clk.Now(), slice3.SliceKey, model.SliceUploading, model.SliceUploaded).Do(ctx).Err())
	}

	// Rotate (5) - AllocatedDiskSpace is calculated from the uploaded slice3 statistics
	// -----------------------------------------------------------------------------------------------------------------
	{
		expectedSlice5Size := int(slice3DiskSize) * file.LocalStorage.DiskAllocation.Relative / 100
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
