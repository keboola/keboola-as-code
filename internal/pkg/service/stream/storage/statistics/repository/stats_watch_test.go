package repository_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func TestRepository_GetAllAndWatch(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, _ := dependencies.NewMockedStorageScope(t)
	repo := d.StatisticsRepository()

	// Add 2 records
	sliceKey1 := test.NewSliceKeyOpenedAt("2000-01-01T01:00:00.000Z")
	sliceKey2 := test.NewSliceKeyOpenedAt("2000-01-01T02:00:00.000Z")
	assert.NoError(t, repo.Put(ctx, "test-node", []statistics.PerSlice{
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
	}))

	// GetAll phase
	ch := repo.GetAllAndWatch(ctx).Channel()
	v, ok := <-ch
	assert.True(t, ok)
	assert.Equal(t, []statistics.Value{
		{
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T02:00:00.000Z"),
			RecordsCount:     1,
			UncompressedSize: 1,
			CompressedSize:   1,
		},
		{
			FirstRecordAt:    utctime.MustParse("2000-01-01T02:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T03:00:00.000Z"),
			RecordsCount:     10,
			UncompressedSize: 10,
			CompressedSize:   10,
		},
	}, watchResponseToSlice(v))

	// Add record
	sliceKey3 := test.NewSliceKeyOpenedAt("2000-01-01T03:00:00.000Z")
	assert.NoError(t, repo.Put(ctx, "test-node", []statistics.PerSlice{
		{
			SliceKey:         sliceKey3,
			FirstRecordAt:    utctime.MustParse("2000-01-01T03:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
			RecordsCount:     100,
			UncompressedSize: 100,
			CompressedSize:   100,
		},
	}))

	// Transition from GetAll to Watch phase
	v, ok = <-ch
	assert.True(t, ok)
	assert.True(t, v.Created)
	assert.Empty(t, v.Events)

	// Watch phase
	v, ok = <-ch
	assert.True(t, ok)
	assert.Equal(t, []statistics.Value{
		{
			FirstRecordAt:    utctime.MustParse("2000-01-01T03:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
			RecordsCount:     100,
			UncompressedSize: 100,
			CompressedSize:   100,
		},
	}, watchResponseToSlice(v))

	// Close the watch stream
	cancel()
	_, ok = <-ch
	assert.False(t, ok)
}

func watchResponseToSlice(resp etcdop.WatchResponseE[etcdop.WatchEventT[statistics.Value]]) (out []statistics.Value) {
	for _, e := range resp.Events {
		out = append(out, e.Value)
	}
	return out
}
