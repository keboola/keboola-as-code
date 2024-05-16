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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

// TestProvider tests repository.Provider interface implemented by the repository.Repository.
func TestRepository_FilesStats(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, _ := dependencies.NewMockedTableSinkScope(t)
	repo := d.StatisticsRepository()

	// Fixtures
	sliceKey1 := test.NewSliceKeyOpenedAt("2000-01-01T01:00:00.000Z")
	sliceKey2 := test.NewSliceKeyOpenedAt("2000-01-01T02:00:00.000Z")
	sliceKey3 := test.NewSliceKeyOpenedAt("2000-01-01T03:00:00.000Z")
	sliceKey4 := test.NewSliceKeyOpenedAt("2000-01-02T01:00:00.000Z")

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
		{
			SliceKey: sliceKey4,
			Value: statistics.Value{
				SlicesCount:      2,
				FirstRecordAt:    utctime.MustParse("2000-01-02T01:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-02T02:00:00.000Z"),
				RecordsCount:     2,
				UncompressedSize: 2,
				CompressedSize:   2,
			},
		},
	}))
	assert.NoError(t, repo.Move(sliceKey2, level.Local, level.Staging).Do(ctx).Err())
	assert.NoError(t, repo.Move(sliceKey3, level.Local, level.Target).Do(ctx).Err())

	// Files Stats
	start := model.FileID{
		OpenedAt: utctime.MustParse("2000-01-01T00:00:00.000Z"),
	}
	end := model.FileID{
		OpenedAt: utctime.MustParse("2000-01-02T00:00:00.000Z"),
	}
	result, err := repo.FilesStats(sliceKey1.SinkKey, start, end).Do(ctx).ResultOrErr()
	require.NoError(t, err)

	// Expected result
	expected := map[model.FileID]*statistics.Aggregated{}
	expected[model.FileID{OpenedAt: utctime.MustParse("2000-01-01T19:00:00.000Z")}] = &statistics.Aggregated{
		Local: statistics.Value{
			SlicesCount:      3,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-02T02:00:00.000Z"),
			RecordsCount:     3,
			UncompressedSize: 3,
			CompressedSize:   3,
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
			SlicesCount:      5,
			FirstRecordAt:    utctime.MustParse("2000-01-01T01:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-02T02:00:00.000Z"),
			RecordsCount:     113,
			UncompressedSize: 113,
			CompressedSize:   113,
		},
	}

	assert.Equal(t, expected, result)
}
