package repository_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestRepository_Move_SameLevels_Panic(t *testing.T) {
	t.Parallel()

	d, _ := dependencies.NewMockedTableSinkScope(t)
	repo := d.StatisticsRepository()

	assert.PanicsWithError(t, `"from" and "to" storage levels are same and equal to "staging"`, func() {
		repo.Move(test.NewSliceKey(), level.Staging, level.Staging)
	})
}

func TestRepository_Move(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, mocked := dependencies.NewMockedTableSinkScope(t)
	client := mocked.EtcdClient()
	repo := d.StatisticsRepository()

	sliceKey := test.NewSliceKeyOpenedAt("2000-01-01T01:00:00.000Z")

	addStagingSize := func(value *statistics.Value) {
		value.StagingSize = 1
	}

	// Move non-existing statistics or empty statistics
	require.NoError(t, repo.Move(sliceKey, level.Staging, level.Target).Do(ctx).Err())
	etcdhelper.AssertKVsString(t, client, "")

	// Create a record in the level.Local
	require.NoError(t, repo.Put(ctx, []statistics.PerSlice{
		{
			SliceKey: sliceKey,
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
`)

	// Move record to the storage.Staging
	sum, err := repo.Move(sliceKey, level.Local, level.Staging, addStagingSize).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, statistics.Value{
		SlicesCount:      1,
		FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-01T02:00:00.000Z"),
		RecordsCount:     1,
		UncompressedSize: 1,
		CompressedSize:   1,
		StagingSize:      1,
	}, sum)
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/stats/staging/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T01:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T01:00:00.000Z",
  "lastRecordAt": "2000-01-01T02:00:00.000Z",
  "recordsCount": 1,
  "uncompressedSize": "1B",
  "compressedSize": "1B",
  "stagingSize": "1B"
}
>>>>>
`)

	// Move record to the level.Target
	require.NoError(t, repo.Move(sliceKey, level.Staging, level.Target).Do(ctx).Err())
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
  "compressedSize": "1B",
  "stagingSize": "1B"
}
>>>>>
`)
}

func TestRepository_MoveAll(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, mocked := dependencies.NewMockedTableSinkScope(t)
	client := mocked.EtcdClient()
	repo := d.StatisticsRepository()

	sliceKey1 := test.NewSliceKeyOpenedAt("2000-01-01T01:00:00.000Z")
	sliceKey2 := test.NewSliceKeyOpenedAt("2000-01-01T02:00:00.000Z")
	sourceKey := sliceKey1.SourceKey
	assert.Equal(t, sourceKey, sliceKey2.SourceKey)

	addStagingSize := func(value *statistics.Value) {
		value.StagingSize = 1
	}

	// Move non-existing statistics or empty statistics
	require.NoError(t, repo.MoveAll(sourceKey, level.Staging, level.Target).Do(ctx).Err())
	etcdhelper.AssertKVsString(t, client, "")

	// Create a record in the level.Local and storage.Staging
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
		{
			SliceKey: sliceKey2,
			Value: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T02:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T03:00:00.000Z"),
				RecordsCount:     2,
				UncompressedSize: 2,
				CompressedSize:   2,
			},
		},
	}))
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
storage/stats/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T02:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T02:00:00.000Z",
  "lastRecordAt": "2000-01-01T03:00:00.000Z",
  "recordsCount": 2,
  "uncompressedSize": "2B",
  "compressedSize": "2B"
}
>>>>>
`)

	// Move sliceKey2 record from the level.Local to the storage.Staging
	require.NoError(t, repo.Move(sliceKey2, level.Local, level.Staging, addStagingSize).Do(ctx).Err())
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
  "firstRecordAt": "2000-01-01T02:00:00.000Z",
  "lastRecordAt": "2000-01-01T03:00:00.000Z",
  "recordsCount": 2,
  "uncompressedSize": "2B",
  "compressedSize": "2B",
  "stagingSize": "1B"
}
>>>>>
`)

	// MoveAll records (sliceKey1) from the level.Local to the storage.Staging
	require.NoError(t, repo.MoveAll(sourceKey, level.Local, level.Staging, addStagingSize).Do(ctx).Err())
	etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/stats/staging/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T01:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T01:00:00.000Z",
  "lastRecordAt": "2000-01-01T02:00:00.000Z",
  "recordsCount": 1,
  "uncompressedSize": "1B",
  "compressedSize": "1B",
  "stagingSize": "1B"
}
>>>>>

<<<<<
storage/stats/staging/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T02:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T02:00:00.000Z",
  "lastRecordAt": "2000-01-01T03:00:00.000Z",
  "recordsCount": 2,
  "uncompressedSize": "2B",
  "compressedSize": "2B",
  "stagingSize": "1B"
}
>>>>>
`)

	// MoveAll records (sliceKey1, sliceKey2) from the level.Staging to the level.Target
	sum, err := repo.MoveAll(sourceKey, level.Staging, level.Target).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, statistics.Value{
		SlicesCount:      2,
		FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-01T03:00:00.000Z"),
		RecordsCount:     3,
		UncompressedSize: 3,
		CompressedSize:   3,
		StagingSize:      2,
	}, sum)
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
  "compressedSize": "1B",
  "stagingSize": "1B"
}
>>>>>

<<<<<
storage/stats/target/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T02:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-01T02:00:00.000Z",
  "lastRecordAt": "2000-01-01T03:00:00.000Z",
  "recordsCount": 2,
  "uncompressedSize": "2B",
  "compressedSize": "2B",
  "stagingSize": "1B"
}
>>>>>
`)
}
