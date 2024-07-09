package csv_test

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/encoder/csv"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testcase"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type fileCompression struct {
	Name        string
	Config      compression.Config
	FileDecoder func(t *testing.T, r io.Reader) io.Reader
}

func TestCSVWriter_CastToStringError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Create slice
	slice := test.NewSlice()
	slice.Type = model.FileTypeCSV
	slice.Columns = column.Columns{column.UUID{Name: "id"}, column.Body{Name: "body"}} // <<<<< two columns
	val := validator.New()
	require.NoError(t, val.Validate(ctx, slice))

	// Create writer
	w, err := csv.NewEncoder(0, io.Discard, slice)
	require.NoError(t, err)

	// Write invalid number of values
	err = w.WriteRecord([]any{struct{}{}})
	if assert.Error(t, err) {
		assert.Equal(t, `cannot convert value of the column "id" to the string: unable to cast struct {}{} of type struct {} to string`, err.Error())
	}
}

// nolint:tparallel // false positive
func TestCSVWriter(t *testing.T) {
	t.Parallel()

	compressions := []fileCompression{
		{
			Name:   "none",
			Config: compression.NewNoneConfig(),
		},
		{
			Name:   "gzip",
			Config: compression.NewGZIPConfig(),
			FileDecoder: func(t *testing.T, r io.Reader) io.Reader {
				t.Helper()
				r, err := gzip.NewReader(r)
				require.NoError(t, err)
				return r
			},
		},
		{
			Name:   "zstd",
			Config: compression.NewZSTDConfig(),
			FileDecoder: func(t *testing.T, r io.Reader) io.Reader {
				t.Helper()
				r, err := zstd.NewReader(r)
				require.NoError(t, err)
				return r
			},
		},
	}

	syncModes := []writesync.Mode{
		writesync.ModeDisabled,
		writesync.ModeDisk,
		writesync.ModeCache,
	}

	// Generate all possible combinations of the parameters
	// nolint:paralleltest // false positive
	for _, comp := range compressions {
		for _, syncMode := range syncModes {
			for _, syncWait := range []bool{false, true} {
				for _, parallelWrite := range []bool{false, true} {
					// Skip invalid combination
					if syncMode == writesync.ModeDisabled && syncWait {
						continue
					}

					// Run test case
					if tc := newTestCase(comp, syncMode, syncWait, parallelWrite); tc != nil {
						t.Run(tc.Name, tc.Run)
					}
				}
			}
		}
	}
}

func newTestCase(comp fileCompression, syncMode writesync.Mode, syncWait bool, parallelWrite bool) *testcase.WriterTestCase {
	// Input rows
	data := []testcase.RowBatch{
		{
			Parallel: parallelWrite,
			Rows:     [][]any{{"abc", 123}, {`"def"`, 456}},
		},
		{
			Parallel: parallelWrite,
			Rows:     [][]any{{"foo", "bar"}, {`xyz`, false}},
		},
	}

	// Expected file content
	var validateFn func(t *testing.T, fileContent string)
	if parallelWrite {
		validateFn = func(t *testing.T, fileContent string) {
			t.Helper()
			assert.Equal(t, 4, strings.Count(fileContent, "\n"))
			assert.Contains(t, fileContent, "\"abc\",\"123\"\n")
			assert.Contains(t, fileContent, "\"\"\"def\"\"\",\"456\"\n")
			assert.Contains(t, fileContent, "\"foo\",\"bar\"\n")
			assert.Contains(t, fileContent, "\"xyz\",\"false\"\n")
		}
	} else {
		validateFn = func(t *testing.T, fileContent string) {
			t.Helper()
			assert.Equal(t, "\"abc\",\"123\"\n\"\"\"def\"\"\",\"456\"\n\"foo\",\"bar\"\n\"xyz\",\"false\"\n", fileContent)
		}
	}

	// Set sync interval
	var intervalTrigger time.Duration
	if syncWait {
		// Trigger sync by the intervalTrigger if syncWait=true
		intervalTrigger = 10 * time.Millisecond
	} else {
		// Other writes are not blocking, sync is triggered by the writer Close.
		intervalTrigger = 1 * time.Second
	}

	// Sync config
	var syncConfig writesync.Config
	switch syncMode {
	case writesync.ModeDisabled:
		syncConfig = writesync.Config{Mode: writesync.ModeDisabled}
	case writesync.ModeDisk:
		syncConfig = writesync.Config{
			Mode:                     writesync.ModeDisk,
			Wait:                     syncWait,
			CheckInterval:            duration.From(5 * time.Millisecond),
			CountTrigger:             5000,
			UncompressedBytesTrigger: 10 * datasize.MB,
			CompressedBytesTrigger:   1 * datasize.MB,
			IntervalTrigger:          duration.From(intervalTrigger),
		}
	case writesync.ModeCache:
		syncConfig = writesync.Config{
			Mode:                     writesync.ModeCache,
			Wait:                     syncWait,
			CheckInterval:            duration.From(5 * time.Millisecond),
			CountTrigger:             5000,
			UncompressedBytesTrigger: 10 * datasize.MB,
			CompressedBytesTrigger:   1 * datasize.MB,
			IntervalTrigger:          duration.From(intervalTrigger),
		}
	default:
		panic(errors.Errorf(`unexpected mode "%v"`, syncMode))
	}

	return &testcase.WriterTestCase{
		Name:     fmt.Sprintf("compression-%s-sync-%s-wait-%t-parallel-%t", comp.Name, syncMode, syncWait, parallelWrite),
		FileType: model.FileTypeCSV,
		Columns: column.Columns{
			// 2 columns, only the count is important for CSV
			column.Body{},
			column.Template{},
		},
		Allocate:    1 * datasize.MB,
		Sync:        syncConfig,
		Compression: comp.Config,
		Data:        data,
		FileDecoder: comp.FileDecoder,
		Validator:   validateFn,
	}
}
