package statistics_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

type testCase struct {
	name     string
	value    statistics.Value
	addition statistics.Value
	expected statistics.Value
}

func TestValue_Add(t *testing.T) {
	t.Parallel()

	testCases := []testCase{
		{
			name:     "empty values",
			value:    statistics.Value{},
			addition: statistics.Value{},
			expected: statistics.Value{},
		},
		{
			name:  "add to empty",
			value: statistics.Value{},
			addition: statistics.Value{
				FirstRecordAt:    utctime.MustParse("2000-01-10T00:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-20T00:00:00.000Z"),
				RecordsCount:     100,
				UncompressedSize: 2000,
				CompressedSize:   300,
			},
			expected: statistics.Value{
				FirstRecordAt:    utctime.MustParse("2000-01-10T00:00:00.000Z"),
				LastRecordAt:     utctime.MustParse("2000-01-20T00:00:00.000Z"),
				RecordsCount:     100,
				UncompressedSize: 2000,
				CompressedSize:   300,
			},
		},
		{
			name: "earlier first record",
			value: statistics.Value{
				FirstRecordAt: utctime.MustParse("2000-01-10T00:00:00.000Z"),
				LastRecordAt:  utctime.MustParse("2000-01-20T00:00:00.000Z"),
			},
			addition: statistics.Value{
				FirstRecordAt: utctime.MustParse("2000-01-05T00:00:00.000Z"),
			},
			expected: statistics.Value{
				FirstRecordAt: utctime.MustParse("2000-01-05T00:00:00.000Z"),
				LastRecordAt:  utctime.MustParse("2000-01-20T00:00:00.000Z"),
			},
		},
		{
			name: "later last record",
			value: statistics.Value{
				FirstRecordAt: utctime.MustParse("2000-01-10T00:00:00.000Z"),
				LastRecordAt:  utctime.MustParse("2000-01-20T00:00:00.000Z"),
			},
			addition: statistics.Value{
				LastRecordAt: utctime.MustParse("2000-01-30T00:00:00.000Z"),
			},
			expected: statistics.Value{
				FirstRecordAt: utctime.MustParse("2000-01-10T00:00:00.000Z"),
				LastRecordAt:  utctime.MustParse("2000-01-30T00:00:00.000Z"),
			},
		},
		{
			name: "increment values",
			value: statistics.Value{
				RecordsCount:     100,
				UncompressedSize: 2000,
				CompressedSize:   300,
			},
			addition: statistics.Value{
				RecordsCount:     1,
				UncompressedSize: 1,
				CompressedSize:   1,
			},
			expected: statistics.Value{
				RecordsCount:     101,
				UncompressedSize: 2001,
				CompressedSize:   301,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.expected, tc.value.Add(tc.addition))
		})
	}
}
