package repository_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/jonboulle/clockwork"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
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
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

func TestRepository_EstimateSliceSizeOnSliceCreate(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	clk := clockwork.NewFakeClockAt(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())
	by := test.ByUser()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}
	ignoredKeys := etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/all/|storage/slice/all/|storage/volume/|storage/stats/")

	// Get services
	d, mocked := dependencies.NewMockedStorageScope(t, ctx, commonDeps.WithClock(clk))
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

	// Create branch, source
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(ctx).Err())
	}

	// Create sink, file, slice
	// -----------------------------------------------------------------------------------------------------------------
	var createEtcdLogs string
	{
		sink := dummy.NewSinkWithLocalStorage(sinkKey)
		etcdLogs.Reset()
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), by, "Create sink").Do(ctx).Err())
		createEtcdLogs = etcdLogs.String()
	}
	{
		// Etcd state
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/stats_alloc_on_create_snapshot.txt", ignoredKeys)
	}
	{
		// Etcd operations
		etcdlogger.AssertFromFile(t, "fixtures/stats_alloc_on_create_ops.txt", createEtcdLogs)
	}

	// Create 2 more files/slices (3 totally)
	// -----------------------------------------------------------------------------------------------------------------
	var sliceKey1, sliceKey2 model.SliceKey
	{
		clk.Advance(time.Hour)
		require.NoError(t, fileRepo.Rotate(sinkKey, clk.Now()).Do(ctx).Err())

		clk.Advance(time.Hour)
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
		require.NoError(t, statsRepo.Put(ctx, "test-node", []statistics.PerSlice{
			{
				SliceKey:         sliceKey1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T02:00:00.000Z"),
				RecordsCount:     123,
				UncompressedSize: 20000,
				CompressedSize:   1000, // <<<<<<<<<<<
			},
			{
				SliceKey:         sliceKey2,
				FirstRecordAt:    utctime.MustParse("2000-01-01T02:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T03:00:00.000Z"),
				RecordsCount:     345,
				UncompressedSize: 10000,
				CompressedSize:   500,
			},
		}))
	}

	// Move slice1 to storage.Staging, move slice2 to storage.Target
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Advance(time.Hour)
		require.NoError(t, sliceRepo.SwitchToUploading(sliceKey1, clk.Now(), false).Do(ctx).Err())
		require.NoError(t, sliceRepo.SwitchToUploading(sliceKey2, clk.Now(), false).Do(ctx).Err())
		require.NoError(t, sliceRepo.SwitchToUploaded(sliceKey1, clk.Now()).Do(ctx).Err())
		require.NoError(t, sliceRepo.SwitchToUploaded(sliceKey2, clk.Now()).Do(ctx).Err())

		clk.Advance(time.Hour)
		require.NoError(t, fileRepo.SwitchToImporting(sliceKey2.FileKey, clk.Now(), false).Do(ctx).Err())
		require.NoError(t, fileRepo.SwitchToImported(sliceKey2.FileKey, clk.Now()).Do(ctx).Err())
	}

	// Create file4/slice4 - pre-allocated disk space should be calculated from the slice1, CompressedSize:   1000
	// -----------------------------------------------------------------------------------------------------------------
	var rotateEtcdLogs string
	{
		clk.Advance(time.Hour)

		etcdLogs.Reset()
		file4, err := fileRepo.Rotate(sinkKey, clk.Now()).Do(ctx).ResultOrErr()
		require.NoError(t, err)
		rotateEtcdLogs = etcdLogs.String()
		require.True(t, file4.LocalStorage.Allocation.Enabled)
		require.Equal(t, uint(110), file4.LocalStorage.Allocation.Relative)

		slices, err := sliceRepo.ListIn(file4.FileKey).Do(ctx).All()
		require.NoError(t, err)
		require.Len(t, slices, 1)
		slice4 := slices[0]
		require.Equal(t, datasize.ByteSize(1000*110/100), slice4.LocalStorage.AllocatedDiskSpace)
	}
	{
		// Etcd state
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/stats_alloc_on_rotate_snapshot.txt", ignoredKeys)
	}
	{
		// Etcd operations
		etcdlogger.AssertFromFile(t, "fixtures/stats_alloc_on_rotate_ops.txt", rotateEtcdLogs)
	}
}
