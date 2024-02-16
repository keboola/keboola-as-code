package repository_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

// TestRepository_Delete_LevelLocalAndStaging tests deletion
// of the statistics on the storage.LevelLocal and storage.Staging.
//
// Statistics are permanently deleted without rollup to the higher level,
// because the data are lost, they did not arrive to the storage.Target.
func TestRepository_Delete_LevelLocalAndStaging(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, mocked := dependencies.NewMockedTableSinkScope(t)
	client := mocked.EtcdClient()
	repo := d.StatisticsRepository()

	// Create records
	sliceKey1 := test.NewSliceKeyOpenedAt("2000-01-01T01:00:00.000Z")
	sliceKey2 := test.NewSliceKeyOpenedAt("2000-01-01T02:00:00.000Z")
	sliceKey3 := test.NewSliceKeyOpenedAt("2000-01-01T03:00:00.000Z")
	value := statistics.Value{
		SlicesCount:      1,
		FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-01T02:00:00.000Z"),
		RecordsCount:     1,
		UncompressedSize: 1,
		CompressedSize:   1,
	}
	assert.NoError(t, repo.Put(ctx, []statistics.PerSlice{
		{SliceKey: sliceKey1, Value: value},
		{SliceKey: sliceKey2, Value: value},
		{SliceKey: sliceKey3, Value: value},
	}))

	// Move statistics for slices 2 and 3 to the storage.Staging
	assert.NoError(t, repo.Move(sliceKey2, level.Local, level.Staging).Do(ctx).Err())
	assert.NoError(t, repo.Move(sliceKey3, level.Local, level.Staging).Do(ctx).Err())

	// Check initial state
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/stats/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T01:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T01:00:00.000Z",
  "lastRecordAt": "2000-01-01T02:00:00.000Z",
  "recordsCount": 1,
  "uncompressedSize": "1B",
  "compressedSize": "1B"
}
>>>>>

<<<<<
storage/stats/staging/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T02:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T01:00:00.000Z",
  "lastRecordAt": "2000-01-01T02:00:00.000Z",
  "recordsCount": 1,
  "uncompressedSize": "1B",
  "compressedSize": "1B"
}
>>>>>

<<<<<
storage/stats/staging/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T03:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T01:00:00.000Z",
  "lastRecordAt": "2000-01-01T02:00:00.000Z",
  "recordsCount": 1,
  "uncompressedSize": "1B",
  "compressedSize": "1B"
}
>>>>>
`)

	// Delete slice 2 statistics
	assert.NoError(t, repo.Delete(sliceKey2).Do(ctx).Err())
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/stats/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T01:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T01:00:00.000Z",
  "lastRecordAt": "2000-01-01T02:00:00.000Z",
  "recordsCount": 1,
  "uncompressedSize": "1B",
  "compressedSize": "1B"
}
>>>>>

<<<<<
storage/stats/staging/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T03:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T01:00:00.000Z",
  "lastRecordAt": "2000-01-01T02:00:00.000Z",
  "recordsCount": 1,
  "uncompressedSize": "1B",
  "compressedSize": "1B"
}
>>>>>
`)

	// Delete file statistics
	assert.NoError(t, repo.Delete(sliceKey1.FileKey).Do(ctx).Err())
	etcdhelper.AssertKVsString(t, client, ``)
}

// TestRepository_Delete_LevelTarget_Sum tests that statistics of data in storage.Target
// are rolled up to the parent object sum when the object is deleted.
func TestRepository_Delete_LevelTarget_Sum(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, mocked := dependencies.NewMockedTableSinkScope(t)
	client := mocked.EtcdClient()
	repo := d.StatisticsRepository()

	// Create records
	sliceKey1 := test.NewSliceKeyOpenedAt("2000-01-01T01:00:00.000Z")
	sliceKey2 := test.NewSliceKeyOpenedAt("2000-01-01T02:00:00.000Z")
	sliceKey3 := test.NewSliceKeyOpenedAt("2000-01-01T03:00:00.000Z")
	assert.NoError(t, repo.Put(ctx, []statistics.PerSlice{
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

	// Move statistics to the target level
	assert.NoError(t, repo.Move(sliceKey1, level.Local, level.Target).Do(ctx).Err())
	assert.NoError(t, repo.Move(sliceKey2, level.Local, level.Target).Do(ctx).Err())
	assert.NoError(t, repo.Move(sliceKey3, level.Local, level.Target).Do(ctx).Err())

	// Check initial state
	if stats, err := repo.ProjectStats(ctx, sliceKey1.ProjectID); assert.NoError(t, err) {
		assert.Equal(t, statistics.Value{
			SlicesCount:      3,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
			RecordsCount:     111,
			UncompressedSize: 111,
			CompressedSize:   111,
		}, stats.Total)
	}
	if stats, err := repo.SourceStats(ctx, sliceKey1.SourceKey); assert.NoError(t, err) {
		assert.Equal(t, statistics.Value{
			SlicesCount:      3,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
			RecordsCount:     111,
			UncompressedSize: 111,
			CompressedSize:   111,
		}, stats.Total)
	}
	if stats, err := repo.SinkStats(ctx, sliceKey1.SinkKey); assert.NoError(t, err) {
		assert.Equal(t, statistics.Value{
			SlicesCount:      3,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
			RecordsCount:     111,
			UncompressedSize: 111,
			CompressedSize:   111,
		}, stats.Total)
	}
	if stats, err := repo.FileStats(ctx, sliceKey1.FileKey); assert.NoError(t, err) {
		assert.Equal(t, statistics.Value{
			SlicesCount:      3,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
			RecordsCount:     111,
			UncompressedSize: 111,
			CompressedSize:   111,
		}, stats.Total)
	}
	if stats, err := repo.SliceStats(ctx, sliceKey1); assert.NoError(t, err) {
		assert.Equal(t, statistics.Value{
			SlicesCount:      1,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T02:00:00.000Z"),
			RecordsCount:     1,
			UncompressedSize: 1,
			CompressedSize:   1,
		}, stats.Total)
	}
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/stats/target/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T01:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T01:00:00.000Z",
  "lastRecordAt": "2000-01-01T02:00:00.000Z",
  "recordsCount": 1,
  "uncompressedSize": "1B",
  "compressedSize": "1B"
}
>>>>>

<<<<<
storage/stats/target/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T02:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T02:00:00.000Z",
  "lastRecordAt": "2000-01-01T03:00:00.000Z",
  "recordsCount": 10,
  "uncompressedSize": "10B",
  "compressedSize": "10B"
}
>>>>>

<<<<<
storage/stats/target/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T03:00:00.000Z/value
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

	// Delete slice 1 statistics
	assert.NoError(t, repo.Delete(sliceKey1).Do(ctx).Err())
	if stats, err := repo.ProjectStats(ctx, sliceKey1.ProjectID); assert.NoError(t, err) {
		assert.Equal(t, statistics.Value{
			SlicesCount:      3,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
			RecordsCount:     111,
			UncompressedSize: 111,
			CompressedSize:   111,
		}, stats.Total)
	}
	if stats, err := repo.SourceStats(ctx, sliceKey1.SourceKey); assert.NoError(t, err) {
		assert.Equal(t, statistics.Value{
			SlicesCount:      3,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
			RecordsCount:     111,
			UncompressedSize: 111,
			CompressedSize:   111,
		}, stats.Total)
	}
	if stats, err := repo.SinkStats(ctx, sliceKey1.SinkKey); assert.NoError(t, err) {
		assert.Equal(t, statistics.Value{
			SlicesCount:      3,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
			RecordsCount:     111,
			UncompressedSize: 111,
			CompressedSize:   111,
		}, stats.Total)
	}
	if stats, err := repo.FileStats(ctx, sliceKey1.FileKey); assert.NoError(t, err) {
		assert.Equal(t, statistics.Value{
			SlicesCount:      3,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
			RecordsCount:     111,
			UncompressedSize: 111,
			CompressedSize:   111,
		}, stats.Total)
	}
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/stats/target/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/_sum
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T01:00:00.000Z",
  "lastRecordAt": "2000-01-01T02:00:00.000Z",
  "recordsCount": 1,
  "uncompressedSize": "1B",
  "compressedSize": "1B"
}
>>>>>

<<<<<
storage/stats/target/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T02:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T02:00:00.000Z",
  "lastRecordAt": "2000-01-01T03:00:00.000Z",
  "recordsCount": 10,
  "uncompressedSize": "10B",
  "compressedSize": "10B"
}
>>>>>

<<<<<
storage/stats/target/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T03:00:00.000Z/value
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

	// Delete slice 3 statistics
	assert.NoError(t, repo.Delete(sliceKey3).Do(ctx).Err())
	if stats, err := repo.ProjectStats(ctx, sliceKey1.ProjectID); assert.NoError(t, err) {
		assert.Equal(t, statistics.Value{
			SlicesCount:      3,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
			RecordsCount:     111,
			UncompressedSize: 111,
			CompressedSize:   111,
		}, stats.Total)
	}
	if stats, err := repo.SourceStats(ctx, sliceKey1.SourceKey); assert.NoError(t, err) {
		assert.Equal(t, statistics.Value{
			SlicesCount:      3,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
			RecordsCount:     111,
			UncompressedSize: 111,
			CompressedSize:   111,
		}, stats.Total)
	}
	if stats, err := repo.SinkStats(ctx, sliceKey1.SinkKey); assert.NoError(t, err) {
		assert.Equal(t, statistics.Value{
			SlicesCount:      3,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
			RecordsCount:     111,
			UncompressedSize: 111,
			CompressedSize:   111,
		}, stats.Total)
	}
	if stats, err := repo.FileStats(ctx, sliceKey1.FileKey); assert.NoError(t, err) {
		assert.Equal(t, statistics.Value{
			SlicesCount:      3,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
			RecordsCount:     111,
			UncompressedSize: 111,
			CompressedSize:   111,
		}, stats.Total)
	}
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/stats/target/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/_sum
-----
{
  "slicesCount": 2,
  "firstRecordAt": "2000-01-01T01:00:00.000Z",
  "lastRecordAt": "2000-01-01T04:00:00.000Z",
  "recordsCount": 101,
  "uncompressedSize": "101B",
  "compressedSize": "101B"
}
>>>>>

<<<<<
storage/stats/target/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T02:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T02:00:00.000Z",
  "lastRecordAt": "2000-01-01T03:00:00.000Z",
  "recordsCount": 10,
  "uncompressedSize": "10B",
  "compressedSize": "10B"
}
>>>>>
`)

	// Delete file
	assert.NoError(t, repo.Delete(sliceKey1.FileKey).Do(ctx).Err())
	if stats, err := repo.ProjectStats(ctx, sliceKey1.ProjectID); assert.NoError(t, err) {
		assert.Equal(t, statistics.Value{
			SlicesCount:      3,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
			RecordsCount:     111,
			UncompressedSize: 111,
			CompressedSize:   111,
		}, stats.Total)
	}
	if stats, err := repo.SourceStats(ctx, sliceKey1.SourceKey); assert.NoError(t, err) {
		assert.Equal(t, statistics.Value{
			SlicesCount:      3,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
			RecordsCount:     111,
			UncompressedSize: 111,
			CompressedSize:   111,
		}, stats.Total)
	}
	if stats, err := repo.SinkStats(ctx, sliceKey1.SinkKey); assert.NoError(t, err) {
		assert.Equal(t, statistics.Value{
			SlicesCount:      3,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
			RecordsCount:     111,
			UncompressedSize: 111,
			CompressedSize:   111,
		}, stats.Total)
	}
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/stats/target/123/456/my-source/my-sink/_sum
-----
{
  "slicesCount": 3,
  "firstRecordAt": "2000-01-01T01:00:00.000Z",
  "lastRecordAt": "2000-01-01T04:00:00.000Z",
  "recordsCount": 111,
  "uncompressedSize": "111B",
  "compressedSize": "111B"
}
>>>>>
`)

	// Delete export
	assert.NoError(t, repo.Delete(sliceKey1.SinkKey).Do(ctx).Err())
	if stats, err := repo.ProjectStats(ctx, sliceKey1.ProjectID); assert.NoError(t, err) {
		assert.Equal(t, statistics.Value{
			SlicesCount:      3,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
			RecordsCount:     111,
			UncompressedSize: 111,
			CompressedSize:   111,
		}, stats.Total)
	}
	if stats, err := repo.SourceStats(ctx, sliceKey1.SourceKey); assert.NoError(t, err) {
		assert.Equal(t, statistics.Value{
			SlicesCount:      3,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
			RecordsCount:     111,
			UncompressedSize: 111,
			CompressedSize:   111,
		}, stats.Total)
	}
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/stats/target/123/456/my-source/_sum
-----
{
  "slicesCount": 3,
  "firstRecordAt": "2000-01-01T01:00:00.000Z",
  "lastRecordAt": "2000-01-01T04:00:00.000Z",
  "recordsCount": 111,
  "uncompressedSize": "111B",
  "compressedSize": "111B"
}
>>>>>
`)

	// Delete receiver
	assert.NoError(t, repo.Delete(sliceKey1.SourceKey).Do(ctx).Err())
	if stats, err := repo.ProjectStats(ctx, sliceKey1.ProjectID); assert.NoError(t, err) {
		assert.Equal(t, statistics.Value{
			SlicesCount:      3,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
			RecordsCount:     111,
			UncompressedSize: 111,
			CompressedSize:   111,
		}, stats.Total)
	}
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/stats/target/123/456/_sum
-----
{
  "slicesCount": 3,
  "firstRecordAt": "2000-01-01T01:00:00.000Z",
  "lastRecordAt": "2000-01-01T04:00:00.000Z",
  "recordsCount": 111,
  "uncompressedSize": "111B",
  "compressedSize": "111B"
}
>>>>>
`)
}
