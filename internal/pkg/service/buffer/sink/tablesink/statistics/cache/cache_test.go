package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/statistics/cache"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/statistics/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

type testCase struct {
	Description string
	Prepare     func()
	Assert      func(repository.Provider)
}

func TestCaches(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clk := clock.NewMock()
	d := dependencies.NewMocked(t, dependencies.WithClock(clk), dependencies.WithEnabledEtcdClient())
	client := d.EtcdClient()
	repo := repository.New(d)

	l1Cache, err := cache.NewL1Cache(d.Logger(), repo)
	require.NoError(t, err)
	defer l1Cache.Stop()

	l2Config := statistics.NewConfig().L2Cache
	l2Cache, err := cache.NewL2Cache(d.Logger(), clk, l1Cache, l2Config)
	require.NoError(t, err)
	defer l2Cache.Stop()

	sliceKey1 := test.NewSliceKeyOpenedAt("2000-01-01T01:00:00.000Z")
	sliceKey2 := test.NewSliceKeyOpenedAt("2000-01-01T02:00:00.000Z")
	sliceKey3 := test.NewSliceKeyOpenedAt("2000-01-01T03:00:00.000Z")

	// Define test cases
	cases := []testCase{
		{
			Description: "Empty",
			Assert: func(provider repository.Provider) {
				stats, err := provider.SinkStats(ctx, sliceKey1.SinkKey)
				require.NoError(t, err)
				assert.Empty(t, stats)
			},
		},
		{
			Description: "Add record (1)",
			Prepare: func() {
				require.NoError(t, repo.Put(ctx, []statistics.PerSlice{
					{
						SliceKey: sliceKey1,
						Value: statistics.Value{
							SlicesCount:      1,
							FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
							LastRecordAt:     utctime.MustParse("2000-01-01T02:00:00.000Z"),
							RecordsCount:     1,
							UncompressedSize: 1,
							CompressedSize:   1,
						},
					},
				}))
			},
			Assert: func(provider repository.Provider) {
				stats, err := provider.SinkStats(ctx, sliceKey1.SinkKey)
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
					Total: statistics.Value{
						SlicesCount:      1,
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
				require.NoError(t, repo.Put(ctx, []statistics.PerSlice{
					{
						SliceKey: sliceKey2,
						Value: statistics.Value{
							SlicesCount:      1,
							FirstRecordAt:    utctime.MustParse("2000-01-01T02:00:00.000Z"),
							LastRecordAt:     utctime.MustParse("2000-01-01T03:00:00.000Z"),
							RecordsCount:     10,
							UncompressedSize: 10,
							CompressedSize:   10,
						},
					},
				}))
			},
			Assert: func(provider repository.Provider) {
				stats, err := provider.SinkStats(ctx, sliceKey1.SinkKey)
				require.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Local: statistics.Value{
						SlicesCount:      2,
						FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
						LastRecordAt:     utctime.MustParse("2000-01-01T03:00:00.000Z"),
						RecordsCount:     11,
						UncompressedSize: 11,
						CompressedSize:   11,
					},
					Total: statistics.Value{
						SlicesCount:      2,
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
				require.NoError(t, repo.Move(sliceKey2, storage.LevelLocal, storage.LevelStaging).Do(ctx).Err())
			},
			Assert: func(provider repository.Provider) {
				stats, err := provider.SinkStats(ctx, sliceKey1.SinkKey)
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
					},
					Total: statistics.Value{
						SlicesCount:      2,
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
			Description: "Add record (3)",
			Prepare: func() {
				require.NoError(t, repo.Put(ctx, []statistics.PerSlice{
					{
						SliceKey: sliceKey3,
						Value: statistics.Value{
							SlicesCount:      1,
							FirstRecordAt:    utctime.MustParse("2000-01-01T03:00:00.000Z"),
							LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
							RecordsCount:     100,
							UncompressedSize: 100,
							CompressedSize:   100,
						},
					},
				}))
			},
			Assert: func(provider repository.Provider) {
				stats, err := provider.SinkStats(ctx, sliceKey1.SinkKey)
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
					},
					Total: statistics.Value{
						SlicesCount:      3,
						FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
						LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
						RecordsCount:     111,
						UncompressedSize: 111,
						CompressedSize:   111,
					},
				}, stats)
			},
		},
		{
			Description: "Move stats from local -> target level",
			Prepare: func() {
				require.NoError(t, repo.Move(sliceKey3, storage.LevelLocal, storage.LevelTarget).Do(ctx).Err())
			},
			Assert: func(provider repository.Provider) {
				stats, err := provider.SinkStats(ctx, sliceKey1.SinkKey)
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
					},
					Target: statistics.Value{
						SlicesCount:      1,
						FirstRecordAt:    utctime.MustParse("2000-01-01T03:00:00.000Z"),
						LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
						RecordsCount:     100,
						UncompressedSize: 100,
						CompressedSize:   100,
					},
					Total: statistics.Value{
						SlicesCount:      3,
						FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
						LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
						RecordsCount:     111,
						UncompressedSize: 111,
						CompressedSize:   111,
					},
				}, stats)
			},
		},
		{
			Description: "Remove stats from the local level",
			Prepare: func() {
				require.NoError(t, repo.Delete(sliceKey1).Do(ctx).Err())
			},
			Assert: func(provider repository.Provider) {
				stats, err := provider.SinkStats(ctx, sliceKey1.SinkKey)
				require.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Staging: statistics.Value{
						SlicesCount:      1,
						FirstRecordAt:    utctime.MustParse("2000-01-01T02:00:00.000Z"),
						LastRecordAt:     utctime.MustParse("2000-01-01T03:00:00.000Z"),
						RecordsCount:     10,
						UncompressedSize: 10,
						CompressedSize:   10,
					},
					Target: statistics.Value{
						SlicesCount:      1,
						FirstRecordAt:    utctime.MustParse("2000-01-01T03:00:00.000Z"),
						LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
						RecordsCount:     100,
						UncompressedSize: 100,
						CompressedSize:   100,
					},
					Total: statistics.Value{
						SlicesCount:      2,
						FirstRecordAt:    utctime.MustParse("2000-01-01T02:00:00.000Z"),
						LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
						RecordsCount:     110,
						UncompressedSize: 110,
						CompressedSize:   110,
					},
				}, stats)
			},
		},
		{
			Description: "Remove stats from the staging level",
			Prepare: func() {
				require.NoError(t, repo.Delete(sliceKey2).Do(ctx).Err())
			},
			Assert: func(provider repository.Provider) {
				stats, err := provider.SinkStats(ctx, sliceKey1.SinkKey)
				require.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Target: statistics.Value{
						SlicesCount:      1,
						FirstRecordAt:    utctime.MustParse("2000-01-01T03:00:00.000Z"),
						LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
						RecordsCount:     100,
						UncompressedSize: 100,
						CompressedSize:   100,
					},
					Total: statistics.Value{
						SlicesCount:      1,
						FirstRecordAt:    utctime.MustParse("2000-01-01T03:00:00.000Z"),
						LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
						RecordsCount:     100,
						UncompressedSize: 100,
						CompressedSize:   100,
					},
				}, stats)
			},
		},
		{
			Description: "Remove stats from the target level, statistics are rolled up to the export sum",
			Prepare: func() {
				require.NoError(t, repo.Delete(sliceKey3.FileKey).Do(ctx).Err())
			},
			Assert: func(provider repository.Provider) {
				stats, err := provider.SinkStats(ctx, sliceKey1.SinkKey)
				require.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Target: statistics.Value{
						SlicesCount:      1,
						FirstRecordAt:    utctime.MustParse("2000-01-01T03:00:00.000Z"),
						LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
						RecordsCount:     100,
						UncompressedSize: 100,
						CompressedSize:   100,
					},
					Total: statistics.Value{
						SlicesCount:      1,
						FirstRecordAt:    utctime.MustParse("2000-01-01T03:00:00.000Z"),
						LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
						RecordsCount:     100,
						UncompressedSize: 100,
						CompressedSize:   100,
					},
				}, stats)
			},
		},
	}

	// Run test cases
	for _, tc := range cases {
		t.Logf(`Test case "%s"`, tc.Description)

		// Make a modification
		var expectedRevision int64
		if tc.Prepare != nil {
			header := etcdhelper.ExpectModification(t, client, tc.Prepare)
			expectedRevision = header.Revision
		}

		// Wait for cache sync
		if expectedRevision > 0 {
			assert.Eventually(t, func() bool {
				return l1Cache.Revision() >= expectedRevision
			}, time.Second, 10*time.Millisecond)
		}

		// Test cached L1
		tc.Assert(l1Cache)

		// Invalidate L2
		clk.Add(l2Config.InvalidationInterval.Duration())
		if expectedRevision > 0 {
			assert.Eventually(t, func() bool {
				return l2Cache.Revision() >= expectedRevision
			}, time.Second, 10*time.Millisecond)
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
  "compressedSize": "100B"
}
>>>>>
`)
}
