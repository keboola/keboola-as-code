package statistics_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
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
			name: "later first record",
			value: statistics.Value{
				FirstRecordAt: utctime.MustParse("2000-01-10T00:00:00.000Z"),
				LastRecordAt:  utctime.MustParse("2000-01-20T00:00:00.000Z"),
			},
			addition: statistics.Value{
				FirstRecordAt: utctime.MustParse("2000-01-11T00:00:00.000Z"),
			},
			expected: statistics.Value{
				FirstRecordAt: utctime.MustParse("2000-01-10T00:00:00.000Z"),
				LastRecordAt:  utctime.MustParse("2000-01-20T00:00:00.000Z"),
			},
		},
		{
			name: "earlier last record",
			value: statistics.Value{
				FirstRecordAt: utctime.MustParse("2000-01-10T00:00:00.000Z"),
				LastRecordAt:  utctime.MustParse("2000-01-20T00:00:00.000Z"),
			},
			addition: statistics.Value{
				LastRecordAt: utctime.MustParse("2000-01-19T00:00:00.000Z"),
			},
			expected: statistics.Value{
				FirstRecordAt: utctime.MustParse("2000-01-10T00:00:00.000Z"),
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
				SlicesCount:      200,
				UncompressedSize: 2000,
				CompressedSize:   300,
				StagingSize:      1000,
			},
			addition: statistics.Value{
				RecordsCount:     1,
				SlicesCount:      1,
				UncompressedSize: 1,
				CompressedSize:   1,
				StagingSize:      1,
			},
			expected: statistics.Value{
				RecordsCount:     101,
				SlicesCount:      201,
				UncompressedSize: 2001,
				CompressedSize:   301,
				StagingSize:      1001,
			},
		},
		{
			name: "empty reset values",
			value: statistics.Value{
				ResetAt: ptr.Ptr(utctime.MustParse("2000-02-01T00:00:00.000Z")),
			},
			addition: statistics.Value{
				ResetAt: ptr.Ptr(utctime.MustParse("2000-02-01T00:00:00.000Z")),
			},
			expected: statistics.Value{
				ResetAt: ptr.Ptr(utctime.MustParse("2000-02-01T00:00:00.000Z")),
			},
		},
		{
			name: "increment reset values",
			value: statistics.Value{
				ResetAt:          ptr.Ptr(utctime.MustParse("2000-02-01T00:00:00.000Z")),
				RecordsCount:     100,
				SlicesCount:      200,
				UncompressedSize: 2000,
				CompressedSize:   300,
				StagingSize:      1000,
			},
			addition: statistics.Value{
				ResetAt:          ptr.Ptr(utctime.MustParse("2000-02-01T00:00:00.000Z")),
				RecordsCount:     1,
				SlicesCount:      1,
				UncompressedSize: 1,
				CompressedSize:   1,
				StagingSize:      1,
			},
			expected: statistics.Value{
				ResetAt:          ptr.Ptr(utctime.MustParse("2000-02-01T00:00:00.000Z")),
				RecordsCount:     101,
				SlicesCount:      201,
				UncompressedSize: 2001,
				CompressedSize:   301,
				StagingSize:      1001,
			},
		},
		{
			name: "normal value, add reset value",
			value: statistics.Value{
				RecordsCount:     100,
				SlicesCount:      200,
				UncompressedSize: 2000,
				CompressedSize:   300,
				StagingSize:      1000,
			},
			addition: statistics.Value{
				ResetAt:          ptr.Ptr(utctime.MustParse("2000-02-01T00:00:00.000Z")),
				RecordsCount:     1,
				SlicesCount:      1,
				UncompressedSize: 1,
				CompressedSize:   1,
				StagingSize:      1,
			},
			expected: statistics.Value{
				RecordsCount:     99,
				SlicesCount:      199,
				UncompressedSize: 1999,
				CompressedSize:   299,
				StagingSize:      999,
			},
		},
		{
			name: "reset value, add normal value",
			value: statistics.Value{
				ResetAt:          ptr.Ptr(utctime.MustParse("2000-02-01T00:00:00.000Z")),
				RecordsCount:     1,
				SlicesCount:      1,
				UncompressedSize: 1,
				CompressedSize:   1,
				StagingSize:      1,
			},
			addition: statistics.Value{
				RecordsCount:     100,
				SlicesCount:      200,
				UncompressedSize: 2000,
				CompressedSize:   300,
				StagingSize:      1000,
			},
			expected: statistics.Value{
				RecordsCount:     99,
				SlicesCount:      199,
				UncompressedSize: 1999,
				CompressedSize:   299,
				StagingSize:      999,
			},
		},
		{
			name: "normal value, add reset value, underflow",
			value: statistics.Value{
				RecordsCount:     1,
				SlicesCount:      1,
				UncompressedSize: 1,
				CompressedSize:   1,
				StagingSize:      1,
			},
			addition: statistics.Value{
				ResetAt:          ptr.Ptr(utctime.MustParse("2000-02-01T00:00:00.000Z")),
				RecordsCount:     100,
				SlicesCount:      200,
				UncompressedSize: 2000,
				CompressedSize:   300,
				StagingSize:      1000,
			},
			expected: statistics.Value{
				RecordsCount:     0,
				SlicesCount:      0,
				UncompressedSize: 0,
				CompressedSize:   0,
				StagingSize:      0,
			},
		},
		{
			name: "reset value, add normal value, underflow",
			value: statistics.Value{
				ResetAt:          ptr.Ptr(utctime.MustParse("2000-02-01T00:00:00.000Z")),
				RecordsCount:     100,
				SlicesCount:      200,
				UncompressedSize: 2000,
				CompressedSize:   300,
				StagingSize:      1000,
			},
			addition: statistics.Value{
				RecordsCount:     1,
				SlicesCount:      1,
				UncompressedSize: 1,
				CompressedSize:   1,
				StagingSize:      1,
			},
			expected: statistics.Value{
				RecordsCount:     0,
				SlicesCount:      0,
				UncompressedSize: 0,
				CompressedSize:   0,
				StagingSize:      0,
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
