package file_test

import (
	"bytes"
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
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

func TestFileRepository_Delete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey1 := key.SourceKey{BranchKey: branchKey, SourceID: "my-source-1"}
	sinkKey1 := key.SinkKey{SourceKey: sourceKey1, SinkID: "my-sink-1"}
	sinkKey2 := key.SinkKey{SourceKey: sourceKey1, SinkID: "my-sink-2"}
	sinkKey3 := key.SinkKey{SourceKey: sourceKey1, SinkID: "my-sink-3"}

	// Get services
	d, mocked := dependencies.NewMockedLocalStorageScope(t, commonDeps.WithClock(clk))
	client := mocked.TestEtcdClient()
	rb := rollback.New(d.Logger())
	defRepo := d.DefinitionRepository()
	statsRepo := d.StatisticsRepository()
	storageRepo := d.StorageRepository()
	fileRepo := storageRepo.File()
	sliceRepo := storageRepo.Slice()
	volumeRepo := storageRepo.Volume()

	// Log etcd operations
	var etcdLogs bytes.Buffer
	rawClient := d.EtcdClient()
	rawClient.KV = etcdlogger.KVLogWrapper(rawClient.KV, &etcdLogs, etcdlogger.WithMinimal())

	// Create parent branch, source, sinks and tokens
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now()).Do(ctx).Err())
		source1 := test.NewSource(sourceKey1)
		require.NoError(t, defRepo.Source().Create(&source1, clk.Now(), "Create source").Do(ctx).Err())
		sink1 := test.NewSink(sinkKey1)
		sink1.Config = sink1.Config.With(testconfig.LocalVolumeConfig(2, []string{"ssd"}))
		require.NoError(t, defRepo.Sink().Create(&sink1, clk.Now(), "Create sink").Do(ctx).Err())
		sink2 := test.NewSink(sinkKey2)
		sink2.Config = sink2.Config.With(testconfig.LocalVolumeConfig(2, []string{"ssd"}))
		require.NoError(t, defRepo.Sink().Create(&sink2, clk.Now(), "Create sink").Do(ctx).Err())
		sink3 := test.NewSink(sinkKey3)
		sink3.Config = sink3.Config.With(testconfig.LocalVolumeConfig(2, []string{"ssd"}))
		require.NoError(t, defRepo.Sink().Create(&sink3, clk.Now(), "Create sink").Do(ctx).Err())
	}

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		test.RegisterWriterVolumes(t, ctx, volumeRepo, session, 3)
	}

	// Create 3 files, each has 2 slices
	// -----------------------------------------------------------------------------------------------------------------
	var f1, f2, f3 model.FileKey
	var s0101, s0102, s0201, s0202, s0301, s0302 model.SliceKey
	{
		clk.Add(time.Hour)

		files, err := fileRepo.RotateAllIn(rb, clk.Now(), branchKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		require.Len(t, files, 3)
		f1 = files[0].FileKey
		f2 = files[1].FileKey
		f3 = files[2].FileKey

		slices, err := sliceRepo.ListIn(branchKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, slices, 6)
		s0101 = slices[0].SliceKey
		s0102 = slices[1].SliceKey
		s0201 = slices[2].SliceKey
		s0202 = slices[3].SliceKey
		s0301 = slices[4].SliceKey
		s0302 = slices[5].SliceKey
	}

	// Put slices statistics values - only statistics in the Target level should be preserved after deletion
	// -----------------------------------------------------------------------------------------------------------------
	require.NoError(t, statsRepo.Put(ctx, []statistics.PerSlice{
		repository.statsPerSlice(s0101),
		repository.statsPerSlice(s0102),
		repository.statsPerSlice(s0201),
		repository.statsPerSlice(s0202),
		repository.statsPerSlice(s0301),
		repository.statsPerSlice(s0302),
	}))

	// File and slices in the Sink2 are in the Staging level
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(1 * time.Hour)
		test.SwitchSliceStates(t, ctx, clk, sliceRepo, s0201, time.Hour, []model.SliceState{
			model.SliceWriting, model.SliceClosing, model.SliceUploading, model.SliceUploaded,
		})
		test.SwitchSliceStates(t, ctx, clk, sliceRepo, s0202, time.Hour, []model.SliceState{
			model.SliceWriting, model.SliceClosing, model.SliceUploading, model.SliceUploaded,
		})
		test.SwitchFileStates(t, ctx, clk, fileRepo, f2, time.Hour, []model.FileState{
			model.FileWriting, model.FileClosing,
		})
	}

	// File and slices in the Sink3 are in the Target level
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(1 * time.Hour)
		test.SwitchSliceStates(t, ctx, clk, sliceRepo, s0301, time.Hour, []model.SliceState{
			model.SliceWriting, model.SliceClosing, model.SliceUploading, model.SliceUploaded,
		})
		test.SwitchSliceStates(t, ctx, clk, sliceRepo, s0302, time.Hour, []model.SliceState{
			model.SliceWriting, model.SliceClosing, model.SliceUploading, model.SliceUploaded,
		})
		test.SwitchFileStates(t, ctx, clk, fileRepo, f3, time.Hour, []model.FileState{
			model.FileWriting, model.FileClosing, model.FileImporting, model.FileImported,
		})
	}

	// Delete all files
	// -----------------------------------------------------------------------------------------------------------------
	var deleteEtcdLogs string
	{
		require.NoError(t, fileRepo.Delete(f1).Do(ctx).Err())
		require.NoError(t, fileRepo.Delete(f2).Do(ctx).Err())

		etcdLogs.Reset()
		require.NoError(t, fileRepo.Delete(f3).Do(ctx).Err())
		deleteEtcdLogs = etcdLogs.String()
	}

	// Check etcd logs
	// -----------------------------------------------------------------------------------------------------------------
	etcdlogger.Assert(t, `
// Delete - AtomicOp - Read Phase
➡️  TXN
  ➡️  THEN:
  001 ➡️  GET "storage/stats/target/123/456/my-source-1/my-sink-3/_sum"
  002 ➡️  GET ["storage/stats/target/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z/", "storage/stats/target/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z0")
✔️  TXN | succeeded: true

// Delete - AtomicOp - Write Phase
➡️  TXN
  ➡️  IF:
  001 "storage/stats/target/123/456/my-source-1/my-sink-3/_sum" MOD EQUAL 0
  002 ["storage/stats/target/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z/", "storage/stats/target/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z0") MOD GREATER 0
  003 ["storage/stats/target/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z/", "storage/stats/target/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z0") MOD LESS %d
  004 "storage/stats/target/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z/my-volume-2/2000-01-01T02:00:00.000Z/value" MOD GREATER 0
  005 "storage/stats/target/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z/my-volume-3/2000-01-01T02:00:00.000Z/value" MOD GREATER 0
  006 "storage/file/all/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z" VERSION NOT_EQUAL 0
  ➡️  THEN:
  001 ➡️  DEL "storage/file/all/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z"
  002 ➡️  DEL "storage/file/level/local/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z"
  003 ➡️  DEL "storage/file/level/staging/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z"
  004 ➡️  DEL "storage/file/level/target/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z"
  005 ➡️  DEL ["storage/slice/all/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z/", "storage/slice/all/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z0")
  006 ➡️  DEL ["storage/slice/level/local/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z/", "storage/slice/level/local/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z0")
  007 ➡️  DEL ["storage/slice/level/staging/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z/", "storage/slice/level/staging/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z0")
  008 ➡️  DEL ["storage/slice/level/target/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z/", "storage/slice/level/target/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z0")
  009 ➡️  DEL ["storage/stats/local/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z/", "storage/stats/local/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z0")
  010 ➡️  DEL ["storage/stats/staging/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z/", "storage/stats/staging/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z0")
  011 ➡️  PUT "storage/stats/target/123/456/my-source-1/my-sink-3/_sum"
  012 ➡️  DEL ["storage/stats/target/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z/", "storage/stats/target/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z0")
  ➡️  ELSE:
  001 ➡️  TXN
  001   ➡️  IF:
  001   001 "storage/file/all/123/456/my-source-1/my-sink-3/2000-01-01T02:00:00.000Z" VERSION NOT_EQUAL 0
✔️  TXN | succeeded: true
`, deleteEtcdLogs)

	// Check etcd state - only statistics in the Target level should be preserved after deletion
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/stats/target/123/456/my-source-1/my-sink-3/_sum
-----
{
  "slicesCount": 2,
  "firstRecordAt": "2000-01-01T02:00:00.000Z",
  "lastRecordAt": "2000-01-01T02:01:00.000Z",
  "recordsCount": 200,
  "uncompressedSize": "200MB",
  "compressedSize": "200MB",
  "stagingSize": "200MB"
}
>>>>>
`, etcdhelper.WithIgnoredKeyPattern("^definition/|storage/secret/token/|storage/volume"))
}
