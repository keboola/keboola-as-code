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

	d, mock := dependencies.NewMockedStorageScope(t, ctx)
	client := mock.TestEtcdClient()
	repo := d.StatisticsRepository()
	sliceKey := test.NewSliceKeyOpenedAt("2000-01-20T00:00:00.000Z")

	// Empty
	assert.NoError(t, repo.Put(ctx, "test-node", []statistics.PerSlice{}))
	etcdhelper.AssertKVsString(t, d.EtcdClient(), ``)

	// Node1
	nodeID1 := "test-node-1"
	assert.NoError(t, repo.Put(ctx, nodeID1, []statistics.PerSlice{
		{
			SliceKey:         sliceKey,
			FirstRecordAt:    utctime.MustParse("2000-01-20T00:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-21T00:00:00.000Z"),
			RecordsCount:     1,
			UncompressedSize: 1,
			CompressedSize:   1,
		},
	}))
	etcdhelper.AssertKVsFromFile(t, client, "fixtures/stats_put_snapshot_001.txt")

	// Node2
	nodeID2 := "test-node-2"
	assert.NoError(t, repo.Put(ctx, nodeID2, []statistics.PerSlice{
		{
			SliceKey:         sliceKey,
			FirstRecordAt:    utctime.MustParse("2000-01-21T00:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-22T00:00:00.000Z"),
			RecordsCount:     2,
			UncompressedSize: 2,
			CompressedSize:   2,
		},
	}))
	etcdhelper.AssertKVsFromFile(t, client, "fixtures/stats_put_snapshot_002.txt")

	// Many
	nodeID3 := "test-node-3"
	var records []statistics.PerSlice
	start := utctime.MustParse("2000-01-21T00:00:00.000Z")
	for i := 0; i < 150; i++ {
		openedAt := start.Add(time.Duration(i) * time.Second)
		records = append(records, statistics.PerSlice{
			SliceKey:         test.NewSliceKeyOpenedAt(openedAt.String()),
			FirstRecordAt:    openedAt,
			LastRecordAt:     openedAt.Add(time.Hour),
			RecordsCount:     1,
			UncompressedSize: 1,
			CompressedSize:   1,
		})
	}
	assert.Len(t, records, 150)
	assert.NoError(t, repo.Put(ctx, nodeID3, records))
	kvs, err := etcdhelper.DumpAll(ctx, d.EtcdClient())
	assert.NoError(t, err)
	assert.Equal(t, 2+150, len(kvs))
}
