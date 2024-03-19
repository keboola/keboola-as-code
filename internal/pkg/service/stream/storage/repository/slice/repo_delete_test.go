package slice_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
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

func TestSliceRepository_Delete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey1 := key.SourceKey{BranchKey: branchKey, SourceID: "my-source-1"}
	sinkKey1 := key.SinkKey{SourceKey: sourceKey1, SinkID: "my-sink-1"}

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
		sink1.Config = sink1.Config.With(testconfig.LocalVolumeConfig(3, []string{"ssd"}))
		require.NoError(t, defRepo.Sink().Create(&sink1, clk.Now(), "Create sink").Do(ctx).Err())
	}

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		test.RegisterWriterVolumes(t, ctx, volumeRepo, session, 5)
	}

	// Create 3 files, each has 2 slices
	// -----------------------------------------------------------------------------------------------------------------
	var s01, s02, s03 model.SliceKey
	{
		clk.Add(time.Hour)

		files, err := fileRepo.RotateAllIn(rb, clk.Now(), branchKey).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		require.Len(t, files, 1)

		slices, err := sliceRepo.ListIn(branchKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, slices, 3)
		s01 = slices[0].SliceKey
		s02 = slices[1].SliceKey
		s03 = slices[2].SliceKey
	}

	// Put slices statistics values
	// -----------------------------------------------------------------------------------------------------------------
	require.NoError(t, statsRepo.Put(ctx, []statistics.PerSlice{
		statsPerSlice(s01),
		statsPerSlice(s02),
		statsPerSlice(s03),
	}))

	// Slice1 and Slice2 are in the Local level, Slice3 is in the Staging level.
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(1 * time.Hour)
		test.SwitchSliceStates(t, ctx, clk, sliceRepo, s02, time.Hour, []model.SliceState{
			model.SliceWriting, model.SliceClosing,
		})
		test.SwitchSliceStates(t, ctx, clk, sliceRepo, s03, time.Hour, []model.SliceState{
			model.SliceWriting, model.SliceClosing, model.SliceUploading, model.SliceUploaded,
		})
	}

	// Delete all slices
	// -----------------------------------------------------------------------------------------------------------------
	var deleteEtcdLogs string
	{
		require.NoError(t, sliceRepo.Delete(s01).Do(ctx).Err())
		require.NoError(t, sliceRepo.Delete(s02).Do(ctx).Err())

		etcdLogs.Reset()
		require.NoError(t, sliceRepo.Delete(s03).Do(ctx).Err())
		deleteEtcdLogs = etcdLogs.String()
	}

	// Check etcd logs
	// -----------------------------------------------------------------------------------------------------------------
	etcdlogger.Assert(t, `
// Delete - AtomicOp - Read Phase
➡️  TXN
  ➡️  THEN:
  001 ➡️  GET "storage/stats/target/123/456/my-source-1/my-sink-1/2000-01-01T02:00:00.000Z/_sum"
  002 ➡️  GET ["storage/stats/target/123/456/my-source-1/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-5/2000-01-01T02:00:00.000Z/", "storage/stats/target/123/456/my-source-1/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-5/2000-01-01T02:00:00.000Z0")
✔️  TXN | succeeded: true

// Delete - AtomicOp - Write Phase
➡️  TXN
  ➡️  IF:
  001 "storage/stats/target/123/456/my-source-1/my-sink-1/2000-01-01T02:00:00.000Z/_sum" MOD EQUAL 0
  002 ["storage/stats/target/123/456/my-source-1/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-5/2000-01-01T02:00:00.000Z/", "storage/stats/target/123/456/my-source-1/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-5/2000-01-01T02:00:00.000Z0") MOD EQUAL 0
  003 "storage/slice/all/123/456/my-source-1/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-5/2000-01-01T02:00:00.000Z" VERSION NOT_EQUAL 0
  ➡️  THEN:
  001 ➡️  DEL "storage/slice/all/123/456/my-source-1/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-5/2000-01-01T02:00:00.000Z"
  002 ➡️  DEL "storage/slice/level/local/123/456/my-source-1/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-5/2000-01-01T02:00:00.000Z"
  003 ➡️  DEL "storage/slice/level/staging/123/456/my-source-1/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-5/2000-01-01T02:00:00.000Z"
  004 ➡️  DEL "storage/slice/level/target/123/456/my-source-1/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-5/2000-01-01T02:00:00.000Z"
  005 ➡️  DEL ["storage/stats/local/123/456/my-source-1/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-5/2000-01-01T02:00:00.000Z/", "storage/stats/local/123/456/my-source-1/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-5/2000-01-01T02:00:00.000Z0")
  006 ➡️  DEL ["storage/stats/staging/123/456/my-source-1/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-5/2000-01-01T02:00:00.000Z/", "storage/stats/staging/123/456/my-source-1/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-5/2000-01-01T02:00:00.000Z0")
  007 ➡️  DEL ["storage/stats/target/123/456/my-source-1/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-5/2000-01-01T02:00:00.000Z/", "storage/stats/target/123/456/my-source-1/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-5/2000-01-01T02:00:00.000Z0")
  ➡️  ELSE:
  001 ➡️  TXN
  001   ➡️  IF:
  001   001 "storage/slice/all/123/456/my-source-1/my-sink-1/2000-01-01T02:00:00.000Z/my-volume-5/2000-01-01T02:00:00.000Z" VERSION NOT_EQUAL 0
✔️  TXN | succeeded: true
`, deleteEtcdLogs)

	// Check etcd state - no slice is in the Target level, so no statistics are preserved.
	// See also TestFileRepository_Delete
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsString(t, client, "", etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/|storage/secret/token/|storage/volume"))
}

func statsPerSlice(k model.SliceKey) statistics.PerSlice {
	return statistics.PerSlice{
		SliceKey: k,
		Value: statistics.Value{
			SlicesCount:      1,
			FirstRecordAt:    k.OpenedAt(),
			LastRecordAt:     k.OpenedAt().Add(time.Minute),
			RecordsCount:     100,
			UncompressedSize: 100 * datasize.MB,
			CompressedSize:   100 * datasize.MB,
		},
	}
}
