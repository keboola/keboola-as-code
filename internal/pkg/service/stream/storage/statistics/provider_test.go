package statistics_test

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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

type providerTestCase struct {
	Description string
	Prepare     func()
	Assert      func(repository.Provider)
}

// TestStatisticsProviders - repository, L1 and L2 cache.
func TestStatisticsProviders(t *testing.T) {
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
	d, mock := dependencies.NewMockedCoordinatorScope(t, ctx, commonDeps.WithClock(clk))
	client := mock.TestEtcdClient()
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
		sink := dummy.NewSinkWithLocalStorage(sinkKey)
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

	// Define test cases
	nodeID := "test-node"
	cases := []providerTestCase{
		{
			Description: "Empty",
			Assert: func(provider repository.Provider) {
				stats, err := provider.SinkStats(ctx, sinkKey)
				require.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Local: statistics.Value{
						SlicesCount: 3,
					},
					Total: statistics.Value{
						SlicesCount: 3,
					},
				}, stats)
			},
		},
		{
			Description: "Add record (1)",
			Prepare: func() {
				require.NoError(t, statsRepo.Put(ctx, nodeID, []statistics.PerSlice{
					{
						SliceKey:         sliceKey1,
						FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
						LastRecordAt:     utctime.MustParse("2000-01-01T02:00:00.000Z"),
						RecordsCount:     1,
						UncompressedSize: 1,
						CompressedSize:   1,
					},
				}))
			},
			Assert: func(provider repository.Provider) {
				stats, err := provider.SinkStats(ctx, sinkKey)
				require.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Local: statistics.Value{
						SlicesCount:      3,
						FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
						LastRecordAt:     utctime.MustParse("2000-01-01T02:00:00.000Z"),
						RecordsCount:     1,
						UncompressedSize: 1,
						CompressedSize:   1,
					},
					Total: statistics.Value{
						SlicesCount:      3,
						FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
						LastRecordAt:     utctime.MustParse("2000-01-01T02:00:00.000Z"),
						RecordsCount:     1,
						UncompressedSize: 1,
						CompressedSize:   1,
					},
				}, stats)
			},
		},
		{
			Description: "Add record (2)",
			Prepare: func() {
				require.NoError(t, statsRepo.Put(ctx, nodeID, []statistics.PerSlice{
					{
						SliceKey:         sliceKey2,
						FirstRecordAt:    utctime.MustParse("2000-01-01T02:00:00.000Z"),
						LastRecordAt:     utctime.MustParse("2000-01-01T03:00:00.000Z"),
						RecordsCount:     10,
						UncompressedSize: 10,
						CompressedSize:   10,
					},
				}))
			},
			Assert: func(provider repository.Provider) {
				stats, err := provider.SinkStats(ctx, sinkKey)
				require.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Local: statistics.Value{
						SlicesCount:      3,
						FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
						LastRecordAt:     utctime.MustParse("2000-01-01T03:00:00.000Z"),
						RecordsCount:     11,
						UncompressedSize: 11,
						CompressedSize:   11,
					},
					Total: statistics.Value{
						SlicesCount:      3,
						FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
						LastRecordAt:     utctime.MustParse("2000-01-01T03:00:00.000Z"),
						RecordsCount:     11,
						UncompressedSize: 11,
						CompressedSize:   11,
					},
				}, stats)
			},
		},
		{
			Description: "Move stats from local -> staging level",
			Prepare: func() {
				// Disable sink, it triggers closing of the active file
				require.NoError(t, defRepo.Sink().Disable(sinkKey, clk.Now(), by, "some reason").Do(ctx).Err())
				require.NoError(t, sliceRepo.SwitchToUploading(sliceKey2, clk.Now(), false).Do(ctx).Err())
				require.NoError(t, sliceRepo.SwitchToUploaded(sliceKey2, clk.Now()).Do(ctx).Err())
			},
			Assert: func(provider repository.Provider) {
				stats, err := provider.SinkStats(ctx, sinkKey)
				require.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Local: statistics.Value{
						SlicesCount:      2,
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
					Total: statistics.Value{
						SlicesCount:      3,
						FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
						LastRecordAt:     utctime.MustParse("2000-01-01T03:00:00.000Z"),
						RecordsCount:     11,
						UncompressedSize: 11,
						CompressedSize:   11,
						StagingSize:      10,
					},
				}, stats)
			},
		},
		{
			Description: "Add record (3)",
			Prepare: func() {
				require.NoError(t, statsRepo.Put(ctx, nodeID, []statistics.PerSlice{
					{
						SliceKey:         sliceKey3,
						FirstRecordAt:    utctime.MustParse("2000-01-01T03:00:00.000Z"),
						LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
						RecordsCount:     100,
						UncompressedSize: 100,
						CompressedSize:   100,
					},
				}))
			},
			Assert: func(provider repository.Provider) {
				stats, err := provider.SinkStats(ctx, sinkKey)
				require.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Local: statistics.Value{
						SlicesCount:      2,
						FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
						LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
						RecordsCount:     101,
						UncompressedSize: 101,
						CompressedSize:   101,
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
					Total: statistics.Value{
						SlicesCount:      3,
						FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
						LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
						RecordsCount:     111,
						UncompressedSize: 111,
						CompressedSize:   111,
						StagingSize:      10,
					},
				}, stats)
			},
		},
		{
			Description: "Move stats from local -> target level",
			Prepare: func() {
				require.NoError(t, sliceRepo.SwitchToUploading(sliceKey3, clk.Now(), false).Do(ctx).Err())
				require.NoError(t, sliceRepo.SwitchToUploaded(sliceKey3, clk.Now()).Do(ctx).Err())
				require.NoError(t, fileRepo.SwitchToImporting(sliceKey3.FileKey, clk.Now(), false).Do(ctx).Err())
				require.NoError(t, fileRepo.SwitchToImported(sliceKey3.FileKey, clk.Now()).Do(ctx).Err())
			},
			Assert: func(provider repository.Provider) {
				stats, err := provider.SinkStats(ctx, sinkKey)
				require.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
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
				}, stats)
			},
		},
		{
			Description: "Remove stats from the local level",
			Prepare: func() {
				require.NoError(t, fileRepo.Delete(sliceKey1.FileKey, clk.Now()).Do(ctx).Err())
			},
			Assert: func(provider repository.Provider) {
				stats, err := provider.SinkStats(ctx, sinkKey)
				require.NoError(t, err)
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
						SlicesCount:      2,
						FirstRecordAt:    utctime.MustParse("2000-01-01T02:00:00.000Z"),
						LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
						RecordsCount:     110,
						UncompressedSize: 110,
						CompressedSize:   110,
						StagingSize:      110,
					},
				}, stats)
			},
		},
		{
			Description: "Remove stats from the staging level",
			Prepare: func() {
				require.NoError(t, fileRepo.Delete(sliceKey2.FileKey, clk.Now()).Do(ctx).Err())
			},
			Assert: func(provider repository.Provider) {
				stats, err := provider.SinkStats(ctx, sinkKey)
				require.NoError(t, err)
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
				}, stats)
			},
		},
		{
			Description: "Remove stats from the target level, statistics are rolled up to the export sum",
			Prepare: func() {
				require.NoError(t, fileRepo.Delete(sliceKey3.FileKey, clk.Now()).Do(ctx).Err())
			},
			Assert: func(provider repository.Provider) {
				stats, err := provider.SinkStats(ctx, sinkKey)
				require.NoError(t, err)
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
				}, stats)
			},
		},
	}

	l1Cache := d.StatisticsL1Cache()
	l2Cache := d.StatisticsL2Cache()

	// Run test cases
	for _, tc := range cases {
		t.Logf(`Test case "%s"`, tc.Description)

		// Make a modification
		var expectedRevision int64
		if tc.Prepare != nil {
			header := etcdhelper.ExpectModification(t, client, tc.Prepare)
			expectedRevision = header.Revision
		}

		// Test repository
		tc.Assert(statsRepo)

		// Wait for cache sync
		if expectedRevision > 0 {
			assert.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.GreaterOrEqual(c, l1Cache.Revision(), expectedRevision)
			}, 5*time.Second, 100*time.Millisecond)
		}

		// Test cached L1
		tc.Assert(l1Cache)

		// Invalidate L2
		clk.Add(mock.TestConfig().Storage.Statistics.Cache.L2.InvalidationInterval.Duration())
		if expectedRevision > 0 {
			assert.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.GreaterOrEqual(c, l2Cache.Revision(), expectedRevision)
			}, 5*time.Second, 100*time.Millisecond)
		}

		// Test cached L2: twice, cold and warm read
		tc.Assert(l2Cache)
		tc.Assert(l2Cache)
	}

	// Check final etcd state
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/stats/target/123/456/my-source/my-sink/_sum
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T03:00:00.000Z",
  "lastRecordAt": "2000-01-01T04:00:00.000Z",
  "recordsCount": 100,
  "uncompressedSize": "100B",
  "compressedSize": "100B",
  "stagingSize": "100B"
}
>>>>>
`, etcdhelper.WithIgnoredKeyPattern(`^definition/|storage/file/|storage/slice/|storage/volume/`))
}
