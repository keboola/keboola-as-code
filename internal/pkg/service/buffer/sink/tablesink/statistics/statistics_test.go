package statistics_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

func TestValue_Add(t *testing.T) {
	t.Parallel()

	// Create an empty value
	v := statistics.Value{}

	// Add an empty value
	v = v.Add(statistics.Value{})
	assert.Equal(t, statistics.Value{}, v)

	// Add to the empty value
	v = v.Add(statistics.Value{
		FirstRecordAt:    utctime.MustParse("2000-01-10T00:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-20T00:00:00.000Z"),
		RecordsCount:     100,
		UncompressedSize: 2000,
		CompressedSize:   300,
	})
	assert.Equal(t, statistics.Value{
		FirstRecordAt:    utctime.MustParse("2000-01-10T00:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-20T00:00:00.000Z"),
		RecordsCount:     100,
		UncompressedSize: 2000,
		CompressedSize:   300,
	}, v)

	// FirstRecordAt before the original
	v = v.Add(statistics.Value{
		FirstRecordAt: utctime.MustParse("2000-01-05T00:00:00.000Z"),
	})
	assert.Equal(t, statistics.Value{
		FirstRecordAt:    utctime.MustParse("2000-01-05T00:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-20T00:00:00.000Z"),
		RecordsCount:     100,
		UncompressedSize: 2000,
		CompressedSize:   300,
	}, v)

	// LastRecordAt after the original
	v = v.Add(statistics.Value{
		LastRecordAt: utctime.MustParse("2000-01-30T00:00:00.000Z"),
	})
	assert.Equal(t, statistics.Value{
		FirstRecordAt:    utctime.MustParse("2000-01-05T00:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-30T00:00:00.000Z"),
		RecordsCount:     100,
		UncompressedSize: 2000,
		CompressedSize:   300,
	}, v)

	// Increment all values
	v = v.Add(statistics.Value{
		RecordsCount:     1,
		UncompressedSize: 1,
		CompressedSize:   1,
	})
	assert.Equal(t, statistics.Value{
		FirstRecordAt:    utctime.MustParse("2000-01-05T00:00:00.000Z"),
		LastRecordAt:     utctime.MustParse("2000-01-30T00:00:00.000Z"),
		RecordsCount:     101,
		UncompressedSize: 2001,
		CompressedSize:   301,
	}, v)
}
