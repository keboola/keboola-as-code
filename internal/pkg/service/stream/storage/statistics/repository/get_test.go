package repository_test

import (
	"context"
	"testing"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/stretchr/testify/assert"
)

// TestProvider tests repository.Provider interface implemented by the repository.Repository.
func TestProvider(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, _ := dependencies.NewMockedLocalStorageScope(t)
	repo := d.StatisticsRepository()

	// Fixtures
	sliceKey1 := test.NewSliceKeyOpenedAt("2000-01-01T01:00:00.000Z")
	sliceKey2 := test.NewSliceKeyOpenedAt("2000-01-01T02:00:00.000Z")
	sliceKey3 := test.NewSliceKeyOpenedAt("2000-01-01T03:00:00.000Z")

	// Empty
	v, err := repo.ProjectStats(ctx, sliceKey1.ProjectID)
	assert.Empty(t, v)
	assert.NoError(t, err)
	v, err = repo.SourceStats(ctx, sliceKey1.SourceKey)
	assert.Empty(t, v)
	assert.NoError(t, err)
	v, err = repo.SinkStats(ctx, sliceKey1.SinkKey)
	assert.Empty(t, v)
	assert.NoError(t, err)
	v, err = repo.FileStats(ctx, sliceKey1.FileKey)
	assert.Empty(t, v)
	assert.NoError(t, err)
	v, err = repo.SliceStats(ctx, sliceKey1)
	assert.Empty(t, v)
	assert.NoError(t, err)

	// Add some statistics
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
	assert.NoError(t, repo.Move(sliceKey2, level.Local, level.Staging).Do(ctx).Err())
	assert.NoError(t, repo.Move(sliceKey3, level.Local, level.Target).Do(ctx).Err())

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
	assert.NoError(t, err)
	assert.Equal(t, expected, v)
	v, err = repo.SourceStats(ctx, sliceKey1.SourceKey)
	assert.NoError(t, err)
	assert.Equal(t, expected, v)
	v, err = repo.SinkStats(ctx, sliceKey1.SinkKey)
	assert.NoError(t, err)
	assert.Equal(t, expected, v)
	v, err = repo.FileStats(ctx, sliceKey1.FileKey)
	assert.NoError(t, err)
	assert.Equal(t, expected, v)

	// Slice level
	v, err = repo.SliceStats(ctx, sliceKey1)
	assert.NoError(t, err)
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
	assert.NoError(t, err)
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
	assert.NoError(t, err)
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
	assert.NoError(t, repo.Put(ctx, []statistics.PerSlice{
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
	assert.NoError(t, err)
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
	assert.NoError(t, err)
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
	assert.NoError(t, err)
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
