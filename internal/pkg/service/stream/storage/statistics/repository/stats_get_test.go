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
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

// TestProvider tests repository.Provider interface implemented by the repository.Repository.
func TestProvider(t *testing.T) {
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
	d, mocked := dependencies.NewMockedStorageScope(t, commonDeps.WithClock(clk))
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

	// Empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		v, err := statsRepo.ProjectStats(ctx, sliceKey1.ProjectID)
		assert.Empty(t, v)
		assert.NoError(t, err)
		v, err = statsRepo.SourceStats(ctx, sliceKey1.SourceKey)
		assert.Empty(t, v)
		assert.NoError(t, err)
		v, err = statsRepo.SinkStats(ctx, sliceKey1.SinkKey)
		assert.Empty(t, v)
		assert.NoError(t, err)
		v, err = statsRepo.FileStats(ctx, sliceKey1.FileKey)
		assert.Empty(t, v)
		assert.NoError(t, err)
		v, err = statsRepo.SliceStats(ctx, sliceKey1)
		assert.Empty(t, v)
		assert.NoError(t, err)
	}

	// Add some statistics
	// -----------------------------------------------------------------------------------------------------------------
	{
		nodeID1 := "test-node-1"
		assert.NoError(t, statsRepo.OpenSlice(sliceKey1, nodeID1).Do(ctx).Err())
		assert.NoError(t, statsRepo.OpenSlice(sliceKey2, nodeID1).Do(ctx).Err())
		assert.NoError(t, statsRepo.OpenSlice(sliceKey3, nodeID1).Do(ctx).Err())
		assert.NoError(t, statsRepo.Put(ctx, nodeID1, []statistics.PerSlice{
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
				LastRecordAt:     utctime.MustParse("2000-01-01T02:10:00.000Z"),
				RecordsCount:     1,
				UncompressedSize: 1,
				CompressedSize:   1,
			},
			{
				SliceKey:         sliceKey3,
				FirstRecordAt:    utctime.MustParse("2000-01-01T03:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T03:10:00.000Z"),
				RecordsCount:     1,
				UncompressedSize: 1,
				CompressedSize:   1,
			},
		}))
	}
	nodeID2 := "test-node-2"
	assert.NoError(t, statsRepo.OpenSlice(sliceKey2, nodeID2).Do(ctx).Err())
	assert.NoError(t, statsRepo.OpenSlice(sliceKey3, nodeID2).Do(ctx).Err())
	assert.NoError(t, statsRepo.Put(ctx, nodeID2, []statistics.PerSlice{
		{
			SliceKey:         sliceKey2,
			FirstRecordAt:    utctime.MustParse("2000-01-01T02:10:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T03:00:00.000Z"),
			RecordsCount:     9,
			UncompressedSize: 9,
			CompressedSize:   9,
		},
		{
			SliceKey:         sliceKey3,
			FirstRecordAt:    utctime.MustParse("2000-01-01T03:10:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
			RecordsCount:     99,
			UncompressedSize: 99,
			CompressedSize:   99,
		},
	}))

	// Move statistics file2 to the staging, and file3 to the target level
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Disable sink, it triggers closing of the active file
		require.NoError(t, defRepo.Sink().Disable(sinkKey, clk.Now(), by, "some reason").Do(ctx).Err())

		// All slices must be marked as uploaded, to mark the file as imported
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

	// Slice level
	// -----------------------------------------------------------------------------------------------------------------
	{
		v, err := statsRepo.SliceStats(ctx, sliceKey1)
		assert.NoError(t, err)
		assert.Equal(t, statistics.Aggregated{
			Local: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T02:00:00.000Z"),
				RecordsCount:     1,
				UncompressedSize: 1,
				CompressedSize:   1,
			},
			Total: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T02:00:00.000Z"),
				RecordsCount:     1,
				UncompressedSize: 1,
				CompressedSize:   1,
			},
		}, v)
		v, err = statsRepo.SliceStats(ctx, sliceKey2)
		assert.NoError(t, err)
		assert.Equal(t, statistics.Aggregated{
			Staging: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T02:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T03:00:00.000Z"),
				RecordsCount:     10,
				UncompressedSize: 10,
				CompressedSize:   10,
				StagingSize:      10,
			},
			Total: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T02:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T03:00:00.000Z"),
				RecordsCount:     10,
				UncompressedSize: 10,
				CompressedSize:   10,
				StagingSize:      10,
			},
		}, v)
		v, err = statsRepo.SliceStats(ctx, sliceKey3)
		assert.NoError(t, err)
		assert.Equal(t, statistics.Aggregated{
			Target: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T03:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
				RecordsCount:     100,
				UncompressedSize: 100,
				CompressedSize:   100,
				StagingSize:      100,
			},
			Total: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T03:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
				RecordsCount:     100,
				UncompressedSize: 100,
				CompressedSize:   100,
				StagingSize:      100,
			},
		}, v)
	}

	// File level
	// -----------------------------------------------------------------------------------------------------------------
	{
		v, err := statsRepo.FileStats(ctx, sliceKey1.FileKey)
		assert.NoError(t, err)
		assert.Equal(t, statistics.Aggregated{
			Local: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T02:00:00.000Z"),
				RecordsCount:     1,
				UncompressedSize: 1,
				CompressedSize:   1,
			},
			Total: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T02:00:00.000Z"),
				RecordsCount:     1,
				UncompressedSize: 1,
				CompressedSize:   1,
			},
		}, v)
		v, err = statsRepo.FileStats(ctx, sliceKey2.FileKey)
		assert.NoError(t, err)
		assert.Equal(t, statistics.Aggregated{
			Staging: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T02:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T03:00:00.000Z"),
				RecordsCount:     10,
				UncompressedSize: 10,
				CompressedSize:   10,
				StagingSize:      10,
			},
			Total: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T02:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T03:00:00.000Z"),
				RecordsCount:     10,
				UncompressedSize: 10,
				CompressedSize:   10,
				StagingSize:      10,
			},
		}, v)
		v, err = statsRepo.FileStats(ctx, sliceKey3.FileKey)
		assert.NoError(t, err)
		assert.Equal(t, statistics.Aggregated{
			Target: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T03:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
				RecordsCount:     100,
				UncompressedSize: 100,
				CompressedSize:   100,
				StagingSize:      100,
			},
			Total: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T03:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
				RecordsCount:     100,
				UncompressedSize: 100,
				CompressedSize:   100,
				StagingSize:      100,
			},
		}, v)
	}

	// Sink and Source level
	// -----------------------------------------------------------------------------------------------------------------
	{
		expectedTotal := statistics.Aggregated{
			Local: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T02:00:00.000Z"),
				RecordsCount:     1,
				UncompressedSize: 1,
				CompressedSize:   1,
			},
			Staging: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T02:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T03:00:00.000Z"),
				RecordsCount:     10,
				UncompressedSize: 10,
				CompressedSize:   10,
				StagingSize:      10,
			},
			Target: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T03:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
				RecordsCount:     100,
				UncompressedSize: 100,
				CompressedSize:   100,
				StagingSize:      100,
			},
			Total: statistics.Value{
				SlicesCount:      3,
				FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
				RecordsCount:     111,
				UncompressedSize: 111,
				CompressedSize:   111,
				StagingSize:      110,
			},
		}

		v, err := statsRepo.SinkStats(ctx, sliceKey1.SinkKey)
		assert.NoError(t, err)
		assert.Equal(t, expectedTotal, v)

		v, err = statsRepo.SourceStats(ctx, sliceKey1.SourceKey)
		assert.NoError(t, err)
		assert.Equal(t, expectedTotal, v)
	}
}
