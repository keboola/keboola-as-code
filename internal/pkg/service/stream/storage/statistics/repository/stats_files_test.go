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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

func TestRepository_FilesStats(t *testing.T) {
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
	d, mocked := dependencies.NewMockedStorageScope(t, ctx, commonDeps.WithClock(clk))
	client := mocked.TestEtcdClient()
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
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
		test.RegisterWriterVolumes(t, ctx, volumeRepo, session, 4)
	}

	// Create branch, source, sink, file, slices
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(ctx).Err())
		sink := dummy.NewSinkWithLocalStorage(sinkKey)
		sink.Config = testconfig.LocalVolumeConfig(4, []string{"ssd"})
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), by, "Create sink").Do(ctx).Err())
	}

	// Get file slices
	// -----------------------------------------------------------------------------------------------------------------
	var sliceKey1, sliceKey2, sliceKey3, sliceKey4 model.SliceKey
	{
		slices, err := sliceRepo.ListIn(sinkKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, slices, 4)
		sliceKey1 = slices[0].SliceKey
		sliceKey2 = slices[1].SliceKey
		sliceKey3 = slices[2].SliceKey
		sliceKey4 = slices[3].SliceKey
	}

	// Add some statistics
	// -----------------------------------------------------------------------------------------------------------------
	{
		nodeID := "test-node"
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
			{
				SliceKey:         sliceKey4,
				FirstRecordAt:    utctime.MustParse("2000-01-02T01:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-02T04:00:00.000Z"),
				RecordsCount:     1,
				UncompressedSize: 1,
				CompressedSize:   1,
			},
		}))
	}

	// Disable sink, it triggers closing of the active file, so all 4 slices are now in the Closing state
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, defRepo.Sink().Disable(sinkKey, clk.Now(), by, "some reason").Do(ctx).Err())
	}

	// Move slice1 and slice2 to the staging level, so 2 slices are in Uploaded state, 2 in Writing state.
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, sliceRepo.SwitchToUploading(sliceKey1, clk.Now(), false).Do(ctx).Err())
		require.NoError(t, sliceRepo.SwitchToUploading(sliceKey2, clk.Now(), false).Do(ctx).Err())

		clk.Add(time.Hour)
		require.NoError(t, sliceRepo.SwitchToUploaded(sliceKey1, clk.Now()).Do(ctx).Err())
		require.NoError(t, sliceRepo.SwitchToUploaded(sliceKey2, clk.Now()).Do(ctx).Err())
	}

	// Files stats
	// -----------------------------------------------------------------------------------------------------------------
	{
		start := model.FileID{OpenedAt: utctime.MustParse("2000-01-01T00:00:00.000Z")}
		end := model.FileID{OpenedAt: utctime.MustParse("2000-01-02T00:00:00.000Z")}
		result, err := statsRepo.FilesStats(sliceKey1.SinkKey, start, end).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.Equal(t, map[model.FileID]*statistics.Aggregated{
			sliceKey1.FileID: {
				Local: statistics.Value{
					SlicesCount:      2,
					FirstRecordAt:    utctime.MustParse("2000-01-01T03:00:00.000Z"),
					LastRecordAt:     utctime.MustParse("2000-01-02T04:00:00.000Z"),
					RecordsCount:     101,
					UncompressedSize: 101,
					CompressedSize:   101,
					StagingSize:      0,
				},
				Staging: statistics.Value{
					SlicesCount:      2,
					FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
					LastRecordAt:     utctime.MustParse("2000-01-01T03:00:00.000Z"),
					RecordsCount:     11,
					UncompressedSize: 11,
					CompressedSize:   11,
					StagingSize:      11,
				},
				Target: statistics.Value{},
				Total: statistics.Value{
					SlicesCount:      4,
					FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
					LastRecordAt:     utctime.MustParse("2000-01-02T04:00:00.000Z"),
					RecordsCount:     112,
					UncompressedSize: 112,
					CompressedSize:   112,
					StagingSize:      11,
				},
			},
		}, result)
	}
}
