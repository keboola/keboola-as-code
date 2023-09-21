package aggregate

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

func TestAggregate(t *testing.T) {
	t.Parallel()

	result := statistics.Aggregated{}

	// Level Local 1
	Aggregate(storage.LevelLocal, statistics.Value{
		FirstRecordAt:    utctime.MustParse("2000-01-10T00:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-20T00:00:00.000Z"),
		RecordsCount:     1,
		UncompressedSize: 1,
		CompressedSize:   1,
	}, &result)
	assert.Equal(t, &statistics.Aggregated{
		Local: statistics.Value{
			FirstRecordAt:    utctime.MustParse("2000-01-10T00:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-20T00:00:00.000Z"),
			RecordsCount:     1,
			UncompressedSize: 1,
			CompressedSize:   1,
		},
		Total: statistics.Value{
			FirstRecordAt:    utctime.MustParse("2000-01-10T00:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-20T00:00:00.000Z"),
			RecordsCount:     1,
			UncompressedSize: 1,
			CompressedSize:   1,
		},
	}, &result)

	// Level Local 2
	Aggregate(storage.LevelLocal, statistics.Value{
		FirstRecordAt:    utctime.MustParse("2000-01-10T00:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-21T00:00:00.000Z"),
		RecordsCount:     1,
		UncompressedSize: 1,
		CompressedSize:   1,
	}, &result)
	assert.Equal(t, &statistics.Aggregated{
		Local: statistics.Value{
			FirstRecordAt:    utctime.MustParse("2000-01-10T00:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-21T00:00:00.000Z"),
			RecordsCount:     2,
			UncompressedSize: 2,
			CompressedSize:   2,
		},
		Total: statistics.Value{
			FirstRecordAt:    utctime.MustParse("2000-01-10T00:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-21T00:00:00.000Z"), // <<<<<<<<<<<<<
			RecordsCount:     2,
			UncompressedSize: 2,
			CompressedSize:   2,
		},
	}, &result)

	// Level Staging 1
	Aggregate(storage.LevelStaging, statistics.Value{
		FirstRecordAt:    utctime.MustParse("2000-01-15T00:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-16T00:00:00.000Z"),
		RecordsCount:     1,
		UncompressedSize: 1,
		CompressedSize:   1,
		StagingSize:      1,
	}, &result)
	assert.Equal(t, &statistics.Aggregated{
		Local: statistics.Value{
			FirstRecordAt:    utctime.MustParse("2000-01-10T00:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-21T00:00:00.000Z"),
			RecordsCount:     2,
			UncompressedSize: 2,
			CompressedSize:   2,
		},
		Staging: statistics.Value{
			FirstRecordAt:    utctime.MustParse("2000-01-15T00:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-16T00:00:00.000Z"),
			RecordsCount:     1,
			UncompressedSize: 1,
			CompressedSize:   1,
			StagingSize:      1,
		},
		Total: statistics.Value{
			FirstRecordAt:    utctime.MustParse("2000-01-10T00:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-21T00:00:00.000Z"),
			RecordsCount:     3,
			UncompressedSize: 3,
			CompressedSize:   3,
			StagingSize:      1,
		},
	}, &result)

	// Level Staging 2
	Aggregate(storage.LevelStaging, statistics.Value{
		FirstRecordAt:    utctime.MustParse("2000-01-14T00:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-22T00:00:00.000Z"),
		RecordsCount:     1,
		UncompressedSize: 1,
		CompressedSize:   1,
		StagingSize:      1,
	}, &result)
	assert.Equal(t, &statistics.Aggregated{
		Local: statistics.Value{
			FirstRecordAt:    utctime.MustParse("2000-01-10T00:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-21T00:00:00.000Z"),
			RecordsCount:     2,
			UncompressedSize: 2,
			CompressedSize:   2,
		},
		Staging: statistics.Value{
			FirstRecordAt:    utctime.MustParse("2000-01-14T00:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-22T00:00:00.000Z"),
			RecordsCount:     2,
			UncompressedSize: 2,
			CompressedSize:   2,
			StagingSize:      2,
		},
		Total: statistics.Value{
			FirstRecordAt:    utctime.MustParse("2000-01-10T00:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-22T00:00:00.000Z"), // <<<<<<<<<<<<<
			RecordsCount:     4,
			UncompressedSize: 4,
			CompressedSize:   4,
			StagingSize:      2,
		},
	}, &result)

	// Level Target 1
	Aggregate(storage.LevelTarget, statistics.Value{
		FirstRecordAt:    utctime.MustParse("2000-01-18T00:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-19T00:00:00.000Z"),
		RecordsCount:     1,
		UncompressedSize: 1,
		CompressedSize:   1,
		StagingSize:      1,
	}, &result)
	assert.Equal(t, &statistics.Aggregated{
		Local: statistics.Value{
			FirstRecordAt:    utctime.MustParse("2000-01-10T00:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-21T00:00:00.000Z"),
			RecordsCount:     2,
			UncompressedSize: 2,
			CompressedSize:   2,
		},
		Staging: statistics.Value{
			FirstRecordAt:    utctime.MustParse("2000-01-14T00:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-22T00:00:00.000Z"),
			RecordsCount:     2,
			UncompressedSize: 2,
			CompressedSize:   2,
			StagingSize:      2,
		},
		Target: statistics.Value{
			FirstRecordAt:    utctime.MustParse("2000-01-18T00:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-19T00:00:00.000Z"),
			RecordsCount:     1,
			UncompressedSize: 1,
			CompressedSize:   1,
			StagingSize:      1,
		},
		Total: statistics.Value{
			FirstRecordAt:    utctime.MustParse("2000-01-10T00:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-22T00:00:00.000Z"),
			RecordsCount:     5,
			UncompressedSize: 5,
			CompressedSize:   5,
			StagingSize:      3,
		},
	}, &result)

	// Level Target 2
	Aggregate(storage.LevelTarget, statistics.Value{
		FirstRecordAt:    utctime.MustParse("2000-01-09T00:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-23T00:00:00.000Z"),
		RecordsCount:     1,
		UncompressedSize: 1,
		CompressedSize:   1,
		StagingSize:      1,
	}, &result)
	assert.Equal(t, &statistics.Aggregated{
		Local: statistics.Value{
			FirstRecordAt:    utctime.MustParse("2000-01-10T00:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-21T00:00:00.000Z"),
			RecordsCount:     2,
			UncompressedSize: 2,
			CompressedSize:   2,
		},
		Staging: statistics.Value{
			FirstRecordAt:    utctime.MustParse("2000-01-14T00:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-22T00:00:00.000Z"),
			RecordsCount:     2,
			UncompressedSize: 2,
			CompressedSize:   2,
			StagingSize:      2,
		},
		Target: statistics.Value{
			FirstRecordAt:    utctime.MustParse("2000-01-09T00:00:00.000Z"),
			LastRecordAt:     utctime.MustParse("2000-01-23T00:00:00.000Z"),
			RecordsCount:     2,
			UncompressedSize: 2,
			CompressedSize:   2,
			StagingSize:      2,
		},
		Total: statistics.Value{
			FirstRecordAt:    utctime.MustParse("2000-01-09T00:00:00.000Z"), // <<<<<<<<<<<<<
			LastRecordAt:     utctime.MustParse("2000-01-23T00:00:00.000Z"), // <<<<<<<<<<<<<
			RecordsCount:     6,
			UncompressedSize: 6,
			CompressedSize:   6,
			StagingSize:      4,
		},
	}, &result)

	// Unexpected level
	assert.Panics(t, func() {
		Aggregate("foo", statistics.Value{}, &result)
	})
}
