package repository_test

import (
	"bytes"
	"context"
	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
	"go.etcd.io/etcd/client/v3/concurrency"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestRepository_MoveStatisticsOnSliceUpdate(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())
	by := test.ByUser()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}

	// Get services
	d, mocked := dependencies.NewMockedLocalStorageScope(t, commonDeps.WithClock(clk))
	client := mocked.TestEtcdClient()
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	fileRepo := storageRepo.File()
	sliceRepo := storageRepo.Slice()
	volumeRepo := storageRepo.Volume()
	statsRepo := d.StatisticsRepository()

	// Log etcd operations
	var etcdLogs bytes.Buffer
	rawClient := d.EtcdClient()
	rawClient.KV = etcdlogger.KVLogWrapper(rawClient.KV, &etcdLogs, etcdlogger.WithMinimal())

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		test.RegisterWriterVolumes(t, ctx, volumeRepo, session, 1)
	}

	// Create branch, source, sink, file, slice
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(ctx).Err())
		sink := test.NewKeboolaTableSink(sinkKey)
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), by, "Create sink").Do(ctx).Err())
	}

	// Create 2 more files/slices (3 totally)
	// -----------------------------------------------------------------------------------------------------------------
	var sliceKey1, sliceKey2, sliceKey3 model.SliceKey
	{
		clk.Add(time.Hour)
		require.NoError(t, fileRepo.Rotate(sinkKey, clk.Now()).Do(ctx).Err())

		clk.Add(time.Hour)
		require.NoError(t, fileRepo.Rotate(sinkKey, clk.Now()).Do(ctx).Err())

		slices, err := sliceRepo.ListIn(sinkKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, slices, 3)
		sliceKey1 = slices[0].SliceKey
		sliceKey2 = slices[1].SliceKey
		sliceKey3 = slices[2].SliceKey
	}

	// Create stats records
	// -----------------------------------------------------------------------------------------------------------------
	{
		value := statistics.Value{
			SlicesCount:      1,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T02:00:00.000Z"),
			RecordsCount:     1,
			UncompressedSize: 1,
			CompressedSize:   1,
		}
		assert.NoError(t, statsRepo.Put(ctx, []statistics.PerSlice{
			{SliceKey: sliceKey1, Value: value},
			{SliceKey: sliceKey2, Value: value},
			{SliceKey: sliceKey3, Value: value},
		}))
	}

	// Check initial state
	// -----------------------------------------------------------------------------------------------------------------
	{
		etcdhelper.AssertKVsFromFile(t, client, `fixtures/stats_move_snapshot_001.txt`, etcdhelper.WithIgnoredKeyPattern(`^definition/|storage/file/|storage/slice/|storage/volume/`))
	}

	// Move statistics to the target level
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Disable sink, it triggers closing of the active file
		require.NoError(t, defRepo.Sink().Disable(sinkKey, clk.Now(), by, "some reason").Do(ctx).Err())

		clk.Add(time.Hour)
		require.NoError(t, sliceRepo.SwitchToUploading(sliceKey2, clk.Now()).Do(ctx).Err())
		require.NoError(t, sliceRepo.SwitchToUploading(sliceKey3, clk.Now()).Do(ctx).Err())

		clk.Add(time.Hour)
		require.NoError(t, sliceRepo.SwitchToUploaded(sliceKey2, clk.Now()).Do(ctx).Err())
		require.NoError(t, sliceRepo.SwitchToUploaded(sliceKey3, clk.Now()).Do(ctx).Err())

		clk.Add(time.Hour)
		require.NoError(t, fileRepo.SwitchToImporting(sliceKey3.FileKey, clk.Now()).Do(ctx).Err())

		clk.Add(time.Hour)
		require.NoError(t, fileRepo.SwitchToImported(sliceKey3.FileKey, clk.Now()).Do(ctx).Err())
	}

	// Check final state
	// -----------------------------------------------------------------------------------------------------------------
	{
		etcdhelper.AssertKVsFromFile(t, client, `fixtures/stats_move_snapshot_002.txt`, etcdhelper.WithIgnoredKeyPattern(`^definition/|storage/file/|storage/slice/|storage/volume/`))
	}
}
