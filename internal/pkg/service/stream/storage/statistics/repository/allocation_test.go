package repository

import (
	"github.com/c2h5oh/datasize"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRepository_MaxUsedDiskSizeBySliceIn(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, _ := dependencies.NewMockedLocalStorageScope(t)
	repo := d.StatisticsRepository()

	// Fixtures
	sliceKey1 := test.NewSliceKeyOpenedAt("2000-01-01T01:00:00.000Z")
	sliceKey2 := test.NewSliceKeyOpenedAt("2000-01-01T02:00:00.000Z")
	sliceKey3 := test.NewSliceKeyOpenedAt("2000-01-01T03:00:00.000Z")
	sliceKey4 := test.NewSliceKeyOpenedAt("2000-01-01T04:00:00.000Z")
	sliceKey5 := test.NewSliceKeyOpenedAt("2000-01-01T05:00:00.000Z")
	sliceKey6 := test.NewSliceKeyOpenedAt("2000-01-01T06:00:00.000Z")
	sinkKey := sliceKey1.SinkKey

	// Empty
	result, err := repo.MaxUsedDiskSizeBySliceIn(sinkKey, 3).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, datasize.ByteSize(0), result)

	// Put statistics
	assert.NoError(t, repo.Put(ctx, []statistics.PerSlice{
		// Last 3 ------------------------------------------------------------------------------------------------------
		{
			SliceKey: sliceKey1,
			Value: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T02:00:00.000Z"),
				RecordsCount:     1,
				UncompressedSize: 1,
				CompressedSize:   10,
			},
		},
		{
			SliceKey: sliceKey2,
			Value: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T02:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T03:00:00.000Z"),
				RecordsCount:     1,
				UncompressedSize: 1,
				CompressedSize:   500,
			},
		},
		// Last 2 ------------------------------------------------------------------------------------------------------
		{
			SliceKey: sliceKey3,
			Value: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T03:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T04:00:00.000Z"),
				RecordsCount:     1,
				UncompressedSize: 1,
				CompressedSize:   400,
			},
		},
		{
			SliceKey: sliceKey4,
			Value: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T04:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T05:00:00.000Z"),
				RecordsCount:     1,
				UncompressedSize: 1,
				CompressedSize:   20,
			},
		},
		// Last 1 ------------------------------------------------------------------------------------------------------
		{
			SliceKey: sliceKey5,
			Value: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T05:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T06:00:00.000Z"),
				RecordsCount:     1,
				UncompressedSize: 1,
				CompressedSize:   30,
			},
		},
		{
			SliceKey: sliceKey6,
			Value: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T06:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T07:00:00.000Z"),
				RecordsCount:     1,
				UncompressedSize: 1,
				CompressedSize:   300,
			},
		},
	}))

	// Move statistics to the storage.Staging
	require.NoError(t, repo.Move(sliceKey1, level.Local, level.Staging).Do(ctx).Err())
	require.NoError(t, repo.Move(sliceKey2, level.Local, level.Staging).Do(ctx).Err())
	require.NoError(t, repo.Move(sliceKey3, level.Local, level.Staging).Do(ctx).Err())
	require.NoError(t, repo.Move(sliceKey4, level.Local, level.Staging).Do(ctx).Err())
	require.NoError(t, repo.Move(sliceKey5, level.Local, level.Staging).Do(ctx).Err())
	require.NoError(t, repo.Move(sliceKey6, level.Local, level.Staging).Do(ctx).Err())

	// Move statistics to the level.Target
	require.NoError(t, repo.Move(sliceKey2, level.Staging, level.Target).Do(ctx).Err())
	require.NoError(t, repo.Move(sliceKey4, level.Staging, level.Target).Do(ctx).Err())
	require.NoError(t, repo.Move(sliceKey6, level.Staging, level.Target).Do(ctx).Err())

	// Maximum disk size - last 1 record
	result, err = repo.MaxUsedDiskSizeBySliceIn(sinkKey, 1).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, datasize.ByteSize(300), result)

	// Maximum disk size - last 2 records
	result, err = repo.MaxUsedDiskSizeBySliceIn(sinkKey, 2).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, datasize.ByteSize(400), result)

	// Maximum disk size - last 3 records
	result, err = repo.MaxUsedDiskSizeBySliceIn(sinkKey, 3).Do(ctx).ResultOrErr()
	require.NoError(t, err)
	assert.Equal(t, datasize.ByteSize(500), result)
}
