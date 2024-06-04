package repository_test

import (
	"context"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

// TestProvider tests repository.Provider interface implemented by the repository.Repository.
func TestProvider(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, _ := dependencies.NewMockedTableSinkScope(t)
	repo := d.StatisticsRepository()

	// Fixtures
	sliceKey1 := test.NewSliceKeyOpenedAt("2000-01-01T01:00:00.000Z")
	sliceKey2 := test.NewSliceKeyOpenedAt("2000-01-01T02:00:00.000Z")
	sliceKey3 := test.NewSliceKeyOpenedAt("2000-01-01T03:00:00.000Z")

	// Empty
	v, err := repo.ProjectStats(ctx, sliceKey1.ProjectID)
	assert.Empty(t, v)
	require.NoError(t, err)
	v, err = repo.SourceStats(ctx, sliceKey1.SourceKey)
	assert.Empty(t, v)
	require.NoError(t, err)
	v, err = repo.SinkStats(ctx, sliceKey1.SinkKey)
	assert.Empty(t, v)
	require.NoError(t, err)
	v, err = repo.FileStats(ctx, sliceKey1.FileKey)
	assert.Empty(t, v)
	require.NoError(t, err)
	v, err = repo.SliceStats(ctx, sliceKey1)
	assert.Empty(t, v)
	require.NoError(t, err)

	// Add some statistics
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
	require.NoError(t, repo.Move(sliceKey2, level.Local, level.Staging).Do(ctx).Err())
	require.NoError(t, repo.Move(sliceKey3, level.Local, level.Target).Do(ctx).Err())

	// Check provider
	expected := statistics.Aggregated{
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
	}

	// Project-File level
	v, err = repo.ProjectStats(ctx, sliceKey1.ProjectID)
	require.NoError(t, err)
	assert.Equal(t, expected, v)
	v, err = repo.SourceStats(ctx, sliceKey1.SourceKey)
	require.NoError(t, err)
	assert.Equal(t, expected, v)
	v, err = repo.SinkStats(ctx, sliceKey1.SinkKey)
	require.NoError(t, err)
	assert.Equal(t, expected, v)
	v, err = repo.FileStats(ctx, sliceKey1.FileKey)
	require.NoError(t, err)
	assert.Equal(t, expected, v)

	// Slice level
	v, err = repo.SliceStats(ctx, sliceKey1)
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
	}, v)
	v, err = repo.SliceStats(ctx, sliceKey2)
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
		Total: statistics.Value{
			SlicesCount:      1,
			FirstRecordAt:    utctime.MustParse("2000-01-01T02:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T03:00:00.000Z"),
			RecordsCount:     10,
			UncompressedSize: 10,
			CompressedSize:   10,
		},
	}, v)
	v, err = repo.SliceStats(ctx, sliceKey3)
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
	}, v)

	// Add slice from a different export
	sliceKey4 := test.NewSliceKeyOpenedAt("2000-01-01T04:00:00.000Z")
	sliceKey4.SinkID += "-2"
	require.NoError(t, repo.Put(ctx, []statistics.PerSlice{
		{
			SliceKey: sliceKey4,
			Value: statistics.Value{
				SlicesCount:      1,
				FirstRecordAt:    utctime.MustParse("2000-01-01T04:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-01T05:00:00.000Z"),
				RecordsCount:     1000,
				UncompressedSize: 1000,
				CompressedSize:   1000,
			},
		},
	}))

	// Receiver level
	v, err = repo.SourceStats(ctx, sliceKey1.SourceKey)
	require.NoError(t, err)
	assert.Equal(t, statistics.Aggregated{
		Local: statistics.Value{
			SlicesCount:      2,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T05:00:00.000Z"),
			RecordsCount:     1001,
			UncompressedSize: 1001,
			CompressedSize:   1001,
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
			SlicesCount:      4,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T05:00:00.000Z"),
			RecordsCount:     1111,
			UncompressedSize: 1111,
			CompressedSize:   1111,
		},
	}, v)

	// Export level
	v, err = repo.SinkStats(ctx, sliceKey1.SinkKey)
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
	}, v)
	v, err = repo.SinkStats(ctx, sliceKey4.SinkKey)
	require.NoError(t, err)
	assert.Equal(t, statistics.Aggregated{
		Local: statistics.Value{
			SlicesCount:      1,
			FirstRecordAt:    utctime.MustParse("2000-01-01T04:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T05:00:00.000Z"),
			RecordsCount:     1000,
			UncompressedSize: 1000,
			CompressedSize:   1000,
		},
		Total: statistics.Value{
			SlicesCount:      1,
			FirstRecordAt:    utctime.MustParse("2000-01-01T04:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-01T05:00:00.000Z"),
			RecordsCount:     1000,
			UncompressedSize: 1000,
			CompressedSize:   1000,
		},
	}, v)
}

func TestRepository_MaxUsedDiskSizeBySliceIn(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, _ := dependencies.NewMockedTableSinkScope(t)
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
	require.NoError(t, repo.Put(ctx, []statistics.PerSlice{
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
