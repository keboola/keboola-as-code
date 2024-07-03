package repository_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestRepository_OpenSlice(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, mock := dependencies.NewMockedLocalStorageScope(t)
	client := mock.TestEtcdClient()
	repo := d.StatisticsRepository()

	sliceKey := test.NewSliceKey()
	nodeID := "test-node"

	// Open first time
	initialValue, err := repo.OpenSlice(sliceKey, nodeID).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Empty(t, initialValue)
	etcdhelper.AssertKVsFromFile(t, client, "fixtures/stats_open_snapshot_001.txt")

	// Put some values
	require.NoError(t, repo.Put(ctx, nodeID, []statistics.PerSlice{
		{
			SliceKey:         sliceKey,
			FirstRecordAt:    utctime.MustParse("2000-01-01T00:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-02T00:00:00.000Z"),
			RecordsCount:     1,
			UncompressedSize: 1,
			CompressedSize:   1,
		},
	}))

	// Open second time
	initialValue, err = repo.OpenSlice(sliceKey, nodeID).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, statistics.Value{
		FirstRecordAt:    utctime.MustParse("2000-01-01T00:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-02T00:00:00.000Z"),
		RecordsCount:     1,
		UncompressedSize: 1,
		CompressedSize:   1,
	}, initialValue)
	etcdhelper.AssertKVsFromFile(t, client, "fixtures/stats_open_snapshot_002.txt")
}
