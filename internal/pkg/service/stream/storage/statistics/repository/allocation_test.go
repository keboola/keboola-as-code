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
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

func TestRepository_EstimateSliceSizeOnSliceCreate(t *testing.T) {
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
	var sliceKey1, sliceKey2 model.SliceKey
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
	}

	// Create stats records
	// -----------------------------------------------------------------------------------------------------------------
	{
		assert.NoError(t, statsRepo.Put(ctx, []statistics.PerSlice{
			{SliceKey: sliceKey1, Value: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T02:00:00.000Z"),
				RecordsCount:     123,
				UncompressedSize: 20000,
				CompressedSize:   1000, // <<<<<<<<<<<
			}},
			{SliceKey: sliceKey2, Value: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T02:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T03:00:00.000Z"),
				RecordsCount:     345,
				UncompressedSize: 10000,
				CompressedSize:   500,
			}},
		}))
	}

	// Move slice1 to storage.Staging, move slice2 to storage.Target
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		require.NoError(t, sliceRepo.SwitchToUploading(sliceKey1, clk.Now()).Do(ctx).Err())
		require.NoError(t, sliceRepo.SwitchToUploading(sliceKey2, clk.Now()).Do(ctx).Err())
		require.NoError(t, sliceRepo.SwitchToUploaded(sliceKey1, clk.Now()).Do(ctx).Err())
		require.NoError(t, sliceRepo.SwitchToUploaded(sliceKey2, clk.Now()).Do(ctx).Err())

		clk.Add(time.Hour)
		require.NoError(t, fileRepo.SwitchToImporting(sliceKey2.FileKey, clk.Now()).Do(ctx).Err())
		require.NoError(t, fileRepo.SwitchToImported(sliceKey2.FileKey, clk.Now()).Do(ctx).Err())
	}

	// Create file4/slice4 - pre-allocated disk space should be calculated from the slice1, CompressedSize:   1000
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Add(time.Hour)
		file4, err := fileRepo.Rotate(sinkKey, clk.Now()).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		assert.True(t, file4.LocalStorage.DiskAllocation.Enabled)
		assert.Equal(t, 110, file4.LocalStorage.DiskAllocation.Relative)
		slices, err := sliceRepo.ListIn(file4.FileKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, slices, 1)
		slice4 := slices[0]
		assert.Equal(t, datasize.ByteSize(1000*110/100), slice4.LocalStorage.AllocatedDiskSpace)
	}
}
