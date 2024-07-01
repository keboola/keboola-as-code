package repository_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

// TestRepository_RollupStatisticsOnFileDelete_LevelLocalStaging tests deletion
// of the statistics in the level.Local and level.Staging.
//
// Statistics are permanently deleted without rollup to the higher level,
// because the data are lost, they did not arrive to the level.Target.
func TestRepository_RollupStatisticsOnFileDelete_LevelLocalStaging(t *testing.T) {
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
		sink := test.NewSinkWithLocalStorage(sinkKey)
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), by, "Create sink").Do(ctx).Err())
	}

	// Create 2 more files/slices (3 totally)
	// -----------------------------------------------------------------------------------------------------------------
	var fileKey1, fileKey2, fileKey3 model.FileKey
	var sliceKey1, sliceKey2, sliceKey3 model.SliceKey
	{
		clk.Add(time.Hour)
		require.NoError(t, fileRepo.Rotate(sinkKey, clk.Now()).Do(ctx).Err())

		clk.Add(time.Hour)
		require.NoError(t, fileRepo.Rotate(sinkKey, clk.Now()).Do(ctx).Err())

		files, err := fileRepo.ListIn(sinkKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, files, 3)
		fileKey1 = files[0].FileKey
		fileKey2 = files[1].FileKey
		fileKey3 = files[2].FileKey

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
		nodeID := "test-node"
		assert.NoError(t, statsRepo.OpenSlice(sliceKey1, nodeID).Do(ctx).Err())
		assert.NoError(t, statsRepo.OpenSlice(sliceKey2, nodeID).Do(ctx).Err())
		assert.NoError(t, statsRepo.OpenSlice(sliceKey3, nodeID).Do(ctx).Err())
		assert.NoError(t, statsRepo.Put(ctx, nodeID, []statistics.PerSlice{
			{
				SliceKey:         sliceKey1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T02:00:00.000Z"),
				RecordsCount:     1,
				UncompressedSize: 1,
				CompressedSize:   1,
			},
			{
				SliceKey:         sliceKey2,
				FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T02:00:00.000Z"),
				RecordsCount:     1,
				UncompressedSize: 1,
				CompressedSize:   1,
			},
			{
				SliceKey:         sliceKey3,
				FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T02:00:00.000Z"),
				RecordsCount:     1,
				UncompressedSize: 1,
				CompressedSize:   1,
			},
		}))
	}

	// Move slices 1 and 2 to the storage.Staging, it moves also related statistics
	// -----------------------------------------------------------------------------------------------------------------
	{
		require.NoError(t, sliceRepo.SwitchToUploading(sliceKey1, clk.Now()).Do(ctx).Err())
		require.NoError(t, sliceRepo.SwitchToUploading(sliceKey2, clk.Now()).Do(ctx).Err())
		require.NoError(t, sliceRepo.SwitchToUploaded(sliceKey1, clk.Now()).Do(ctx).Err())
		require.NoError(t, sliceRepo.SwitchToUploaded(sliceKey2, clk.Now()).Do(ctx).Err())
	}

	// Check initial state
	// -----------------------------------------------------------------------------------------------------------------
	{
		etcdhelper.AssertKVsFromFile(t, client, `fixtures/stats_delete_snapshot_001.txt`, etcdhelper.WithIgnoredKeyPattern(`^definition/|storage/file/|storage/slice/|storage/volume/`))
	}

	// Delete files
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, fileRepo.Delete(fileKey1, clk.Now()).Do(ctx).Err())
		require.NoError(t, fileRepo.Delete(fileKey2, clk.Now()).Do(ctx).Err())
		require.NoError(t, fileRepo.Delete(fileKey3, clk.Now()).Do(ctx).Err())
	}

	// All statistics have been deleted together with files
	// -----------------------------------------------------------------------------------------------------------------
	{
		etcdhelper.AssertKVsString(t, client, ``, etcdhelper.WithIgnoredKeyPattern(`^definition/|storage/file/|storage/slice/|storage/volume/`))
	}
}

// TestRepository_RollupStatisticsOnFileDelete_LevelTarget tests that statistics of data in level.Target
// are rolled up to the Sink sum when the object is deleted.
func TestRepository_RollupStatisticsOnFileDelete_LevelTarget(t *testing.T) {
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
		sink := test.NewSinkWithLocalStorage(sinkKey)
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), by, "Create sink").Do(ctx).Err())
	}

	// Create 2 more files/slices (3 totally)
	// -----------------------------------------------------------------------------------------------------------------
	var fileKey1, fileKey2, fileKey3 model.FileKey
	var sliceKey1, sliceKey2, sliceKey3 model.SliceKey
	{
		clk.Add(time.Hour)
		require.NoError(t, fileRepo.Rotate(sinkKey, clk.Now()).Do(ctx).Err())

		clk.Add(time.Hour)
		require.NoError(t, fileRepo.Rotate(sinkKey, clk.Now()).Do(ctx).Err())

		files, err := fileRepo.ListIn(sinkKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, files, 3)
		fileKey1 = files[0].FileKey
		fileKey2 = files[1].FileKey
		fileKey3 = files[2].FileKey

		slices, err := sliceRepo.ListIn(sinkKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, slices, 3)
		sliceKey1 = slices[0].SliceKey
		sliceKey2 = slices[1].SliceKey
		sliceKey3 = slices[2].SliceKey
	}

	// Create records
	nodeID := "test-node"
	assert.NoError(t, statsRepo.OpenSlice(sliceKey1, nodeID).Do(ctx).Err())
	assert.NoError(t, statsRepo.OpenSlice(sliceKey2, nodeID).Do(ctx).Err())
	assert.NoError(t, statsRepo.OpenSlice(sliceKey3, nodeID).Do(ctx).Err())
	assert.NoError(t, statsRepo.Put(ctx, nodeID, []statistics.PerSlice{
		{
			SliceKey:         sliceKey1,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T02:00:00.000Z"),
			RecordsCount:     1,
			UncompressedSize: 1,
			CompressedSize:   1,
		},
		{
			SliceKey:         sliceKey2,
			FirstRecordAt:    utctime.MustParse("2000-01-01T02:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T03:00:00.000Z"),
			RecordsCount:     10,
			UncompressedSize: 10,
			CompressedSize:   10,
		},
		{
			SliceKey:         sliceKey3,
			FirstRecordAt:    utctime.MustParse("2000-01-01T03:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
			RecordsCount:     100,
			UncompressedSize: 100,
			CompressedSize:   100,
		},
	}))

	// Move statistics to the target level
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Disable sink, it triggers closing of the active file
		require.NoError(t, defRepo.Sink().Disable(sinkKey, clk.Now(), by, "some reason").Do(ctx).Err())

		clk.Add(time.Hour)
		require.NoError(t, sliceRepo.SwitchToUploading(sliceKey1, clk.Now()).Do(ctx).Err())
		require.NoError(t, sliceRepo.SwitchToUploading(sliceKey2, clk.Now()).Do(ctx).Err())
		require.NoError(t, sliceRepo.SwitchToUploading(sliceKey3, clk.Now()).Do(ctx).Err())

		clk.Add(time.Hour)
		require.NoError(t, sliceRepo.SwitchToUploaded(sliceKey1, clk.Now()).Do(ctx).Err())
		require.NoError(t, sliceRepo.SwitchToUploaded(sliceKey2, clk.Now()).Do(ctx).Err())
		require.NoError(t, sliceRepo.SwitchToUploaded(sliceKey3, clk.Now()).Do(ctx).Err())

		clk.Add(time.Hour)
		require.NoError(t, fileRepo.SwitchToImporting(fileKey1, clk.Now()).Do(ctx).Err())
		require.NoError(t, fileRepo.SwitchToImporting(fileKey2, clk.Now()).Do(ctx).Err())
		require.NoError(t, fileRepo.SwitchToImporting(fileKey3, clk.Now()).Do(ctx).Err())

		clk.Add(time.Hour)
		require.NoError(t, fileRepo.SwitchToImported(fileKey1, clk.Now()).Do(ctx).Err())
		require.NoError(t, fileRepo.SwitchToImported(fileKey2, clk.Now()).Do(ctx).Err())
		require.NoError(t, fileRepo.SwitchToImported(fileKey3, clk.Now()).Do(ctx).Err())
	}

	// Check initial state
	// -----------------------------------------------------------------------------------------------------------------
	{
		etcdhelper.AssertKVsFromFile(t, client, `fixtures/stats_delete_snapshot_002.txt`, etcdhelper.WithIgnoredKeyPattern(`^definition/|storage/file/|storage/slice/|storage/volume/`))
	}

	// Delete files
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, fileRepo.Delete(fileKey1, clk.Now()).Do(ctx).Err())
		require.NoError(t, fileRepo.Delete(fileKey2, clk.Now()).Do(ctx).Err())
		require.NoError(t, fileRepo.Delete(fileKey3, clk.Now()).Do(ctx).Err())
		etcdhelper.AssertKVsFromFile(t, client, `fixtures/stats_delete_snapshot_003.txt`, etcdhelper.WithIgnoredKeyPattern(`^definition/|storage/file/|storage/slice/|storage/volume/`))
	}
}
