package repository_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestRepository_Put(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, _ := dependencies.NewMockedLocalStorageScope(t)
	repo := d.StatisticsRepository()

	// Empty
	assert.NoError(t, repo.Put(ctx, []statistics.PerSlice{}))
	etcdhelper.AssertKVsString(t, d.EtcdClient(), ``)

	// One
	assert.NoError(t, repo.Put(ctx, []statistics.PerSlice{
		{
			SliceKey: test.NewSliceKeyOpenedAt("2000-01-20T00:00:00.000Z"),
			Value: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-20T00:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-21T00:00:00.000Z"),
				RecordsCount:     1,
				UncompressedSize: 1,
				CompressedSize:   1,
			},
		},
	}))
	etcdhelper.AssertKVsString(t, d.EtcdClient(), `
<<<<<
storage/stats/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-20T00:00:00.000Z/value
-----
{
  "slicesCount": 1,
  "firstRecordAt": "2000-01-20T00:00:00.000Z",
  "lastRecordAt": "2000-01-21T00:00:00.000Z",
  "recordsCount": 1,
  "uncompressedSize": "1B",
  "compressedSize": "1B"
}
>>>>>
`)

	// Many
	var records []statistics.PerSlice
	start := utctime.MustParse("2000-01-21T00:00:00.000Z")
	for i := 0; i < 150; i++ {
		openedAt := start.Add(time.Duration(i) * time.Second)
		records = append(records, statistics.PerSlice{
			SliceKey: test.NewSliceKeyOpenedAt(openedAt.String()),
			Value: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    openedAt,
				LastRecordAt:     openedAt.Add(time.Hour),
				RecordsCount:     1,
				UncompressedSize: 1,
				CompressedSize:   1,
			},
		})
	}
	assert.Len(t, records, 150)
	assert.NoError(t, repo.Put(ctx, records))
	kvs, err := etcdhelper.DumpAll(ctx, d.EtcdClient())
	assert.NoError(t, err)
	assert.Len(t, kvs, 151)
}
